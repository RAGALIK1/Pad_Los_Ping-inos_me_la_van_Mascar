// Șablonul Adapter în C#: IMessageAdapter + JsonAdapter + XmlAdapter + AdapterFactory.
using System.Text;
using System.Text.Json;
using System.Xml;
using System.Xml.Linq;

public class MessageFormatException(string message) : Exception(message);

public interface IMessageAdapter
{
    string Name { get; }
    byte[] Serialize(IDictionary<string, string> msg);
    Dictionary<string, string> Deserialize(byte[] data);
}

public sealed class JsonAdapter : IMessageAdapter
{
    public string Name => "json";

    public byte[] Serialize(IDictionary<string, string> msg) =>
        JsonSerializer.SerializeToUtf8Bytes(msg, new JsonSerializerOptions
        {
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        });

    public Dictionary<string, string> Deserialize(byte[] data)
    {
        try
        {
            return JsonSerializer.Deserialize<Dictionary<string, string>>(data)
                   ?? throw new MessageFormatException("JSON gol");
        }
        catch (JsonException e) { throw new MessageFormatException($"JSON invalid: {e.Message}"); }
    }
}

public sealed class XmlAdapter : IMessageAdapter
{
    public string Name => "xml";

    public byte[] Serialize(IDictionary<string, string> msg)
    {
        var root = new XElement("message", msg.Select(kv => new XElement(kv.Key, kv.Value)));
        return Encoding.UTF8.GetBytes(root.ToString(SaveOptions.DisableFormatting));
    }

    public Dictionary<string, string> Deserialize(byte[] data)
    {
        try
        {
            var settings = new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit }; // anti-XXE
            using var reader = XmlReader.Create(new MemoryStream(data), settings);
            var root = XElement.Load(reader);
            if (root.Name != "message") throw new MessageFormatException("rădăcina trebuie să fie <message>");
            return root.Elements().ToDictionary(e => e.Name.LocalName, e => e.Value);
        }
        catch (XmlException e) { throw new MessageFormatException($"XML invalid: {e.Message}"); }
    }
}

public static class AdapterFactory
{
    private static readonly Dictionary<string, IMessageAdapter> Registry = new()
    {
        ["json"] = new JsonAdapter(),
        ["xml"] = new XmlAdapter(),
    };

    public static IMessageAdapter Get(string name) =>
        Registry.TryGetValue(name.ToLowerInvariant(), out var a) ? a : throw new ArgumentException($"format necunoscut: {name}");

    public static IMessageAdapter Detect(byte[] data)
    {
        var head = Encoding.UTF8.GetString(data, 0, Math.Min(16, data.Length)).TrimStart('\uFEFF', ' ', '\t', '\r', '\n');
        if (head.StartsWith('{')) return Registry["json"];
        if (head.StartsWith('<')) return Registry["xml"];
        throw new MessageFormatException("format nedetectabil");
    }
}
