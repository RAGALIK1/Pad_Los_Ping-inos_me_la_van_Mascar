// Client C# pentru agentul de mesagerie.
//   dotnet run -- receive --name cs-recv --format xml --topics "news.*,alerts"
//   dotnet run -- send --name cs-sender --format json --topic alerts --message "alarmă" --count 3
using System.Buffers.Binary;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;

var opts = new Dictionary<string, string>();
for (int i = 1; i < args.Length; i++)
{
    if (!args[i].StartsWith("--")) continue;
    bool flag = i + 1 >= args.Length || args[i + 1].StartsWith("--");
    opts[args[i][2..]] = flag ? "true" : args[++i];
}
string mode = args.Length > 0 ? args[0] : "receive";
string Opt(string k, string d) => opts.TryGetValue(k, out var v) ? v : d;

var adapter = AdapterFactory.Get(Opt("format", "json"));
string name = Opt("name", mode == "send" ? "cs-sender" : "cs-receiver");
var hostPort = Opt("host", "127.0.0.1:5000").Split(':');

using var tcp = new TcpClient { NoDelay = true };
await tcp.ConnectAsync(hostPort[0], int.Parse(hostPort[1]));
var stream = tcp.GetStream();
var writeLock = new SemaphoreSlim(1, 1);

async Task Send(Dictionary<string, string> msg)
{
    var body = adapter.Serialize(msg);
    var frame = new byte[4 + body.Length];
    BinaryPrimitives.WriteUInt32BigEndian(frame, (uint)body.Length); // prefix de lungime
    body.CopyTo(frame, 4);
    await writeLock.WaitAsync();
    try { await stream.WriteAsync(frame); } finally { writeLock.Release(); }
}

async Task<Dictionary<string, string>> Receive()
{
    var hdr = new byte[4];
    await stream.ReadExactlyAsync(hdr);
    uint len = BinaryPrimitives.ReadUInt32BigEndian(hdr);
    if (len > 1 << 20) throw new IOException($"cadru prea mare: {len}");
    var body = new byte[len];
    await stream.ReadExactlyAsync(body);
    return AdapterFactory.Detect(body).Deserialize(body);
}

static string Sha(string s) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(s))).ToLowerInvariant();
static string G(Dictionary<string, string> m, string k) => m.TryGetValue(k, out var v) ? v : "";

await Send(new() { ["type"] = "connect", ["sender"] = name, ["format"] = adapter.Name, ["role"] = mode == "send" ? "sender" : "receiver" });
var ack = await Receive();
if (G(ack, "type") != "connack") { Console.Error.WriteLine($"conectare refuzată: {G(ack, "error")}"); return 2; }
Console.WriteLine($"[c#] conectat ca {name}, format {adapter.Name.ToUpper()}");

bool autoAck = !opts.ContainsKey("no-ack");
var readerTask = Task.Run(async () =>
{
    try
    {
        while (true)
        {
            var m = await Receive();
            switch (G(m, "type"))
            {
                case "deliver":
                    var ok = Sha(G(m, "payload")) == G(m, "checksum") ? "✓ integru" : "✗ ALTERAT";
                    Console.WriteLine($"[c#] ← {G(m, "topic")} de la {G(m, "sender")} (trimis ca {G(m, "format").ToUpper()}, încercarea {G(m, "attempt")}) {ok}\n       {G(m, "payload")}");
                    if (autoAck) await Send(new() { ["type"] = "ack", ["ref"] = G(m, "id") });
                    break;
                case "puback": Console.WriteLine($"[c#] ✓ publicat {G(m, "ref")[..8]} pe {G(m, "topic")}: {G(m, "payload")}"); break;
                case "suback": Console.WriteLine($"[c#] abonat la {G(m, "topic")}"); break;
                case "error": Console.WriteLine($"[c#] eroare broker: {G(m, "error")}"); break;
            }
        }
    }
    catch (Exception e) when (e is IOException or EndOfStreamException or ObjectDisposedException)
    {
        Console.WriteLine("[c#] conexiune închisă");
    }
});

Task Publish(string topic, string payload) => Send(new()
{
    ["type"] = "publish", ["id"] = Guid.NewGuid().ToString(), ["topic"] = topic, ["payload"] = payload,
    ["checksum"] = Sha(payload), ["timestamp"] = DateTime.UtcNow.ToString("O"),
});

if (mode == "send")
{
    string topic = Opt("topic", "news.general");
    if (opts.TryGetValue("message", out var text))
    {
        int count = int.Parse(Opt("count", "1"));
        for (int i = 1; i <= count; i++)
        {
            await Publish(topic, count > 1 ? $"{text} #{i}" : text);
            await Task.Delay(int.Parse(Opt("interval", "300")));
        }
        await Task.Delay(500);
        return 0;
    }
    Console.WriteLine("[c#] scrie mesaje (Enter = trimite). Prefix „@topic ” pentru alt subiect.");
    string? line;
    while ((line = Console.ReadLine()) != null)
    {
        if (string.IsNullOrWhiteSpace(line)) continue;
        if (line.StartsWith('@') && line.Contains(' '))
        {
            topic = line[1..line.IndexOf(' ')];
            line = line[(line.IndexOf(' ') + 1)..];
        }
        await Publish(topic, line);
    }
    await Task.Delay(300);
    return 0;
}

foreach (var t in Opt("topics", "#").Split(','))
    await Send(new() { ["type"] = "subscribe", ["id"] = Guid.NewGuid().ToString(), ["topic"] = t.Trim() });
await readerTask;
return 0;
