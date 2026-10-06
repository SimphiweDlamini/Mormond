using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

namespace Mormond;

public class Program
{
    private static WebSocket? _agentSocket;
    private static readonly object _socketLock = new();
    private static readonly SemaphoreSlim _sendLock = new(1, 1);
    private static readonly ConcurrentDictionary<string, TaskCompletionSource<TunnelResponse>> _pendingRequests = new();
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    public static void Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        var app = builder.Build();

        app.UseWebSockets(new WebSocketOptions
        {
            KeepAliveInterval = TimeSpan.FromSeconds(15)
        });

        // 1. Connection Endpoint for Local Agent Service
        app.Map("/register-agent", async context =>
        {
            if (!context.WebSockets.IsWebSocketRequest)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                await context.Response.WriteAsync("WebSocket connection expected.");
                return;
            }

            var incomingSocket = await context.WebSockets.AcceptWebSocketAsync();
            Console.WriteLine($"[RELAY] Local Agent connecting from {context.Connection.RemoteIpAddress}...");

            // Safely swap and terminate any lingering zombie connection
            WebSocket? oldSocket;
            lock (_socketLock)
            {
                oldSocket = _agentSocket;
                _agentSocket = incomingSocket;
            }

            if (oldSocket != null && oldSocket.State == WebSocketState.Open)
            {
                Console.WriteLine("⚠️ [RELAY] Cleaning up old zombie connection.");
                try { oldSocket.Abort(); } catch { }
            }

            Console.WriteLine("✅ [RELAY] Local Agent connected and registered.");

            var buffer = new byte[1024 * 64];

            try
            {
                while (incomingSocket.State == WebSocketState.Open && !context.RequestAborted.IsCancellationRequested)
                {
                    using var ms = new MemoryStream();
                    WebSocketReceiveResult result;

                    do
                    {
                        result = await incomingSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);

                        if (result.MessageType == WebSocketMessageType.Close)
                        {
                            await incomingSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closing", CancellationToken.None);
                            break;
                        }

                        if (result.MessageType == WebSocketMessageType.Text)
                        {
                            ms.Write(buffer, 0, result.Count);
                        }
                    }
                    while (!result.EndOfMessage);

                    if (result.MessageType == WebSocketMessageType.Text && ms.Length > 0)
                    {
                        string rawJson = Encoding.UTF8.GetString(ms.ToArray());

                        // Handle Application-Level Ping
                        if (rawJson == "PING")
                        {
                            await _sendLock.WaitAsync();
                            try
                            {
                                if (incomingSocket.State == WebSocketState.Open)
                                {
                                    var pongBytes = Encoding.UTF8.GetBytes("PONG");
                                    await incomingSocket.SendAsync(new ArraySegment<byte>(pongBytes), WebSocketMessageType.Text, true, CancellationToken.None);
                                }
                            }
                            finally
                            {
                                _sendLock.Release();
                            }
                            continue;
                        }

                        if (!string.IsNullOrWhiteSpace(rawJson))
                        {
                            try
                            {
                                var responsePacket = JsonSerializer.Deserialize<TunnelResponse>(rawJson, JsonOptions);

                                if (responsePacket != null && !string.IsNullOrEmpty(responsePacket.RequestId))
                                {
                                    if (_pendingRequests.TryRemove(responsePacket.RequestId, out var tcs))
                                    {
                                        tcs.TrySetResult(responsePacket);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"❌ [RELAY] Deserialization error: {ex.Message}");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⚠️ [RELAY] Connection exception: {ex.Message}");
            }
            finally
            {
                lock (_socketLock)
                {
                    if (_agentSocket == incomingSocket)
                    {
                        _agentSocket = null;
                        Console.WriteLine("❌ [RELAY] Local Agent disconnected. Tunnel is offline.");
                    }
                }

                // Fail any active requests waiting on this socket
                foreach (var kvp in _pendingRequests)
                {
                    if (_pendingRequests.TryRemove(kvp.Key, out var tcs))
                    {
                        tcs.TrySetException(new Exception("Tunnel disconnected before response was received."));
                    }
                }
            }
        });

        // 2. Global Proxy Route
        app.Map("{*path}", async context =>
        {
            WebSocket? currentSocket;
            lock (_socketLock)
            {
                currentSocket = _agentSocket;
            }

            if (currentSocket == null || currentSocket.State != WebSocketState.Open)
            {
                context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
                await context.Response.WriteAsync("503 Service Unavailable: Tunnel Offline.");
                return;
            }

            string path = context.Request.Path + context.Request.QueryString;
            string method = context.Request.Method;
            string contentType = context.Request.ContentType ?? string.Empty;

            string bodyBase64 = string.Empty;
            if (context.Request.ContentLength > 0 || method == "POST" || method == "PUT" || method == "PATCH")
            {
                using var bodyMs = new MemoryStream();
                await context.Request.Body.CopyToAsync(bodyMs);
                bodyBase64 = Convert.ToBase64String(bodyMs.ToArray());
            }

            string requestId = Guid.NewGuid().ToString();
            var tcs = new TaskCompletionSource<TunnelResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pendingRequests[requestId] = tcs;

            var outboundPacket = new TunnelRequest
            {
                RequestId = requestId,
                Method = method,
                Path = path,
                ContentType = contentType,
                BodyBase64 = bodyBase64
            };

            var packetBytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(outboundPacket));

            try
            {
                await _sendLock.WaitAsync();
                try
                {
                    if (currentSocket.State == WebSocketState.Open)
                    {
                        await currentSocket.SendAsync(new ArraySegment<byte>(packetBytes), WebSocketMessageType.Text, true, CancellationToken.None);
                    }
                    else
                    {
                        throw new InvalidOperationException("Socket closed prior to dispatch.");
                    }
                }
                finally
                {
                    _sendLock.Release();
                }
            }
            catch (Exception)
            {
                _pendingRequests.TryRemove(requestId, out _);
                context.Response.StatusCode = StatusCodes.Status502BadGateway;
                await context.Response.WriteAsync("502 Bad Gateway: Tunnel Transmission Failure.");
                return;
            }

            // Wait for response or 30s execution timeout
            var completedTask = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(30)));

            if (completedTask == tcs.Task)
            {
                try
                {
                    var responseData = await tcs.Task;
                    context.Response.StatusCode = responseData.StatusCode;
                    context.Response.ContentType = responseData.ContentType;

                    if (!string.IsNullOrEmpty(responseData.BodyBase64))
                    {
                        byte[] rawBinaryData = Convert.FromBase64String(responseData.BodyBase64);
                        await context.Response.Body.WriteAsync(rawBinaryData, 0, rawBinaryData.Length);
                    }
                }
                catch (Exception ex)
                {
                    context.Response.StatusCode = StatusCodes.Status502BadGateway;
                    await context.Response.WriteAsync($"502 Bad Gateway: {ex.Message}");
                }
            }
            else
            {
                _pendingRequests.TryRemove(requestId, out _);
                context.Response.StatusCode = StatusCodes.Status504GatewayTimeout;
                await context.Response.WriteAsync("504 Gateway Timeout: Local system timed out.");
            }
        });

        string port = Environment.GetEnvironmentVariable("PORT") ?? "5000";
        app.Run($"http://0.0.0.0:{port}");
    }
}

public class TunnelRequest
{
    public string RequestId { get; set; } = string.Empty;
    public string Method { get; set; } = string.Empty;
    public string Path { get; set; } = string.Empty;
    public string ContentType { get; set; } = string.Empty;
    public string BodyBase64 { get; set; } = string.Empty;
}

public class TunnelResponse
{
    public string RequestId { get; set; } = string.Empty;
    public int StatusCode { get; set; }
    public string ContentType { get; set; } = string.Empty;
    public string BodyBase64 { get; set; } = string.Empty;
}