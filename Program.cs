using System.Net.WebSockets;
using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;

namespace BackendRelay;

public class Program
{
    private static WebSocket? _agentSocket;
    private static readonly object _sendLock = new();
    private static readonly ConcurrentDictionary<string, DateTime> _activeSessions = new();
    private static readonly TimeSpan MaxConnectionTime = TimeSpan.FromMinutes(15);
    private static readonly ConcurrentDictionary<string, TaskCompletionSource<TunnelResponse>> _pendingRequests = new();
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    public static void Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        var app = builder.Build();

        app.UseWebSockets();

        app.Map("/register-agent", async context =>
        {
            if (context.WebSockets.IsWebSocketRequest)
            {
                _agentSocket = await context.WebSockets.AcceptWebSocketAsync();
                Console.WriteLine("✅ Local Agent securely connected from the office network.");

                var buffer = new byte[1024 * 64];

                try
                {
                    while (_agentSocket.State == WebSocketState.Open)
                    {
                        using var ms = new MemoryStream();
                        WebSocketReceiveResult result;

                        do
                        {
                            result = await _agentSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                            ms.Write(buffer, 0, result.Count);
                        } while (!result.EndOfMessage);

                        if (result.MessageType == WebSocketMessageType.Close)
                        {
                            await _agentSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closing", CancellationToken.None);
                        }
                        else if (result.MessageType == WebSocketMessageType.Text)
                        {
                            string rawJson = Encoding.UTF8.GetString(ms.ToArray());
                            var responsePacket = JsonSerializer.Deserialize<TunnelResponse>(rawJson, JsonOptions);

                            if (responsePacket != null && _pendingRequests.TryRemove(responsePacket.RequestId, out var tcs))
                            {
                                tcs.SetResult(responsePacket);
                            }
                        }
                    }
                }
                catch (WebSocketException)
                {
                    Console.WriteLine("⚠️ Notice: Local Agent connection was severed abruptly.");
                }
                finally
                {
                    _agentSocket = null;
                    Console.WriteLine("❌ Local Agent cleanup complete.");
                }
            }
            else
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
            }
        });

        app.Map("{*path}", async context =>
        {
            if (_agentSocket == null || _agentSocket.State != WebSocketState.Open)
            {
                context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
                await context.Response.WriteAsync("Tunnel Offline.");
                return;
            }

            string userIp = context.Connection.RemoteIpAddress?.ToString() ?? "unknown";
            if (_activeSessions.TryGetValue(userIp, out var sessionStartTime) && DateTime.UtcNow - sessionStartTime > MaxConnectionTime)
            {
                context.Response.StatusCode = StatusCodes.Status403Forbidden;
                await context.Response.WriteAsync("Session Expired.");
                return;
            }
            _activeSessions.TryAdd(userIp, DateTime.UtcNow);

            string path = context.Request.Path + context.Request.QueryString;
            string method = context.Request.Method;
            string contentType = context.Request.ContentType ?? string.Empty;

            // Extract the incoming request body (e.g. login JSON payloads)
            string bodyBase64 = string.Empty;
            if (context.Request.ContentLength > 0 || method == "POST" || method == "PUT" || method == "PATCH")
            {
                using var bodyMs = new MemoryStream();
                await context.Request.Body.CopyToAsync(bodyMs);
                bodyBase64 = Convert.ToBase64String(bodyMs.ToArray());
            }

            string requestId = Guid.NewGuid().ToString();
            var tcs = new TaskCompletionSource<TunnelResponse>();
            _pendingRequests[requestId] = tcs;

            // Package metadata bundle along with headers and incoming body payload
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
                lock (_sendLock)
                {
                    _agentSocket.SendAsync(new ArraySegment<byte>(packetBytes), WebSocketMessageType.Text, true, CancellationToken.None)
                                .GetAwaiter()
                                .GetResult();
                }
            }
            catch (Exception)
            {
                context.Response.StatusCode = StatusCodes.Status502BadGateway;
                await context.Response.WriteAsync("Tunnel Transmission Failure.");
                return;
            }

            var completedTask = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(20)));

            if (completedTask == tcs.Task)
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
            else
            {
                _pendingRequests.TryRemove(requestId, out _);
                context.Response.StatusCode = StatusCodes.Status504GatewayTimeout;
                await context.Response.WriteAsync("Error: Local system timed out.");
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