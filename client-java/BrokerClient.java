import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.*;

/**
 * Client Java pentru agentul de mesagerie (sender + receiver).
 * Nu are dependențe externe; rulează direct cu:
 *
 *   java BrokerClient.java send    --name java-sender --format xml  --topic news.sport --message "Gol!" --count 3
 *   java BrokerClient.java send    --name java-sender --format json --topic news.sport        (interactiv)
 *   java BrokerClient.java receive --name java-recv   --format xml  --topics "news.*,alerts"
 */
public class BrokerClient {

    // ======================================================== Adapter pattern
    interface MessageAdapter {
        String name();
        byte[] serialize(Map<String, String> msg);
        Map<String, String> deserialize(byte[] data) throws FormatException;
    }

    static class FormatException extends Exception {
        FormatException(String m) { super(m); }
    }

    /** JSON pentru obiecte plate cu valori string (parser scris de mână, fără biblioteci). */
    static class JsonAdapter implements MessageAdapter {
        public String name() { return "json"; }

        public byte[] serialize(Map<String, String> msg) {
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, String> e : msg.entrySet()) {
                if (e.getValue() == null) continue;
                if (!first) sb.append(',');
                first = false;
                quote(sb, e.getKey()).append(':');
                quote(sb, e.getValue());
            }
            return sb.append('}').toString().getBytes(StandardCharsets.UTF_8);
        }

        private static StringBuilder quote(StringBuilder sb, String s) {
            sb.append('"');
            for (char c : s.toCharArray()) {
                switch (c) {
                    case '"' -> sb.append("\\\"");
                    case '\\' -> sb.append("\\\\");
                    case '\n' -> sb.append("\\n");
                    case '\r' -> sb.append("\\r");
                    case '\t' -> sb.append("\\t");
                    default -> {
                        if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                        else sb.append(c);
                    }
                }
            }
            return sb.append('"');
        }

        public Map<String, String> deserialize(byte[] data) throws FormatException {
            String s = new String(data, StandardCharsets.UTF_8);
            int[] i = {0};
            Map<String, String> out = new LinkedHashMap<>();
            skipWs(s, i);
            expect(s, i, '{');
            skipWs(s, i);
            if (peek(s, i) == '}') { i[0]++; return out; }
            while (true) {
                skipWs(s, i);
                String key = readString(s, i);
                skipWs(s, i);
                expect(s, i, ':');
                skipWs(s, i);
                if (peek(s, i) != '"') throw new FormatException("câmpul '" + key + "' trebuie să fie string");
                out.put(key, readString(s, i));
                skipWs(s, i);
                char c = s.charAt(i[0]++);
                if (c == '}') break;
                if (c != ',') throw new FormatException("JSON invalid la poziția " + (i[0] - 1));
            }
            return out;
        }

        private static char peek(String s, int[] i) throws FormatException {
            if (i[0] >= s.length()) throw new FormatException("JSON incomplet");
            return s.charAt(i[0]);
        }
        private static void skipWs(String s, int[] i) { while (i[0] < s.length() && Character.isWhitespace(s.charAt(i[0]))) i[0]++; }
        private static void expect(String s, int[] i, char c) throws FormatException {
            if (peek(s, i) != c) throw new FormatException("JSON invalid: se aștepta '" + c + "' la poziția " + i[0]);
            i[0]++;
        }
        private static String readString(String s, int[] i) throws FormatException {
            expect(s, i, '"');
            StringBuilder sb = new StringBuilder();
            while (true) {
                char c = peek(s, i);
                i[0]++;
                if (c == '"') return sb.toString();
                if (c != '\\') { sb.append(c); continue; }
                char e = peek(s, i);
                i[0]++;
                switch (e) {
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case 'u' -> { sb.append((char) Integer.parseInt(s.substring(i[0], i[0] + 4), 16)); i[0] += 4; }
                    default -> sb.append(e);
                }
            }
        }
    }

    /** XML: <message><camp>valoare</camp>...</message>, parsat cu DOM (JDK). */
    static class XmlAdapter implements MessageAdapter {
        public String name() { return "xml"; }

        public byte[] serialize(Map<String, String> msg) {
            StringBuilder sb = new StringBuilder("<message>");
            for (Map.Entry<String, String> e : msg.entrySet()) {
                if (e.getValue() == null) continue;
                sb.append('<').append(e.getKey()).append('>')
                  .append(escape(e.getValue()))
                  .append("</").append(e.getKey()).append('>');
            }
            return sb.append("</message>").toString().getBytes(StandardCharsets.UTF_8);
        }

        private static String escape(String s) {
            return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                    .replace("\"", "&quot;").replace("'", "&apos;");
        }

        public Map<String, String> deserialize(byte[] data) throws FormatException {
            try {
                DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
                f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true); // anti-XXE
                DocumentBuilder b = f.newDocumentBuilder();
                b.setErrorHandler(new org.xml.sax.helpers.DefaultHandler());
                Element root = b.parse(new ByteArrayInputStream(data)).getDocumentElement();
                if (!root.getTagName().equals("message")) throw new FormatException("rădăcina trebuie să fie <message>");
                Map<String, String> out = new LinkedHashMap<>();
                NodeList kids = root.getChildNodes();
                for (int k = 0; k < kids.getLength(); k++) {
                    if (kids.item(k) instanceof Element el) out.put(el.getTagName(), el.getTextContent());
                }
                return out;
            } catch (FormatException e) {
                throw e;
            } catch (Exception e) {
                throw new FormatException("XML invalid: " + e.getMessage());
            }
        }
    }

    static class AdapterFactory {
        private static final Map<String, MessageAdapter> REG = Map.of("json", new JsonAdapter(), "xml", new XmlAdapter());

        static MessageAdapter get(String name) {
            MessageAdapter a = REG.get(name.toLowerCase());
            if (a == null) throw new IllegalArgumentException("format necunoscut: " + name);
            return a;
        }

        static MessageAdapter detect(byte[] data) throws FormatException {
            String s = new String(data, 0, Math.min(data.length, 16), StandardCharsets.UTF_8).strip();
            if (s.startsWith("{")) return REG.get("json");
            if (s.startsWith("<")) return REG.get("xml");
            throw new FormatException("format nedetectabil");
        }
    }

    // ================================================================ rețea
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final MessageAdapter adapter;
    private final Object writeLock = new Object();

    BrokerClient(String host, int port, MessageAdapter adapter) throws IOException {
        this.socket = new Socket(host, port);
        this.socket.setTcpNoDelay(true);
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        this.adapter = adapter;
    }

    void send(Map<String, String> msg) throws IOException {
        byte[] body = adapter.serialize(msg);
        synchronized (writeLock) {
            out.writeInt(body.length);   // prefix de lungime u32 big-endian
            out.write(body);
            out.flush();
        }
    }

    Map<String, String> receive() throws IOException, FormatException {
        int len = in.readInt();
        if (len < 0 || len > 1 << 20) throw new IOException("cadru invalid: " + len);
        byte[] body = new byte[len];
        in.readFully(body);
        return AdapterFactory.detect(body).deserialize(body);
    }

    static Map<String, String> msg(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) m.put(kv[i], kv[i + 1]);
        return m;
    }

    static String sha256(String s) {
        try {
            byte[] h = MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : h) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ================================================================= main
    public static void main(String[] args) throws Exception {
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));
        if (args.length == 0 || !(args[0].equals("send") || args[0].equals("receive"))) {
            System.err.println("utilizare: java BrokerClient.java send|receive [--host h] [--port p] [--name n] [--format json|xml]");
            System.err.println("   send:    --topic t [--message text] [--count n] [--interval ms]");
            System.err.println("   receive: --topics \"a.*,b\" [--no-ack]");
            System.exit(1);
        }
        Map<String, String> o = new HashMap<>();
        for (int i = 1; i < args.length; i++) {
            if (!args[i].startsWith("--")) continue;
            boolean flag = i + 1 >= args.length || args[i + 1].startsWith("--");
            o.put(args[i].substring(2), flag ? "true" : args[++i]);
        }
        boolean sending = args[0].equals("send");
        String name = o.getOrDefault("name", sending ? "java-sender" : "java-receiver");
        MessageAdapter adapter = AdapterFactory.get(o.getOrDefault("format", "json"));
        BrokerClient c = new BrokerClient(o.getOrDefault("host", "127.0.0.1"),
                Integer.parseInt(o.getOrDefault("port", "5000")), adapter);

        c.send(msg("type", "connect", "sender", name, "format", adapter.name(), "role", sending ? "sender" : "receiver"));
        Map<String, String> ack = c.receive();
        if (!"connack".equals(ack.get("type"))) {
            System.err.println("conectare refuzată: " + ack.get("error"));
            System.exit(2);
        }
        System.out.printf("[java] conectat ca %s, format %s%n", name, adapter.name().toUpperCase());

        boolean autoAck = !o.containsKey("no-ack");
        Thread reader = new Thread(() -> {
            try {
                while (true) {
                    Map<String, String> m = c.receive();
                    switch (m.getOrDefault("type", "")) {
                        case "deliver" -> {
                            boolean ok = sha256(m.getOrDefault("payload", "")).equals(m.getOrDefault("checksum", ""));
                            System.out.printf("[java] ← %s  de la %s (trimis ca %s, încercarea %s) %s%n        %s%n",
                                    m.get("topic"), m.get("sender"), m.getOrDefault("format", "?").toUpperCase(),
                                    m.get("attempt"), ok ? "✓ integru" : "✗ ALTERAT", m.get("payload"));
                            if (autoAck) c.send(msg("type", "ack", "ref", m.get("id")));
                        }
                        case "puback" -> System.out.printf("[java] ✓ publicat %s pe %s: %s%n",
                                m.get("ref").substring(0, 8), m.get("topic"), m.get("payload"));
                        case "suback" -> System.out.printf("[java] abonat la %s%n", m.get("topic"));
                        case "error" -> System.out.printf("[java] eroare broker: %s%n", m.get("error"));
                        default -> System.out.println("[java] " + m);
                    }
                }
            } catch (EOFException e) {
                System.out.println("[java] brokerul a închis conexiunea");
                System.exit(0);
            } catch (Exception e) {
                System.out.println("[java] conexiune pierdută: " + e.getMessage());
                System.exit(1);
            }
        }, "reader");
        reader.start();

        if (sending) {
            String topic = o.getOrDefault("topic", "news.general");
            int interval = Integer.parseInt(o.getOrDefault("interval", "300"));
            if (o.containsKey("message")) {
                int count = Integer.parseInt(o.getOrDefault("count", "1"));
                for (int k = 1; k <= count; k++) {
                    String p = count > 1 ? o.get("message") + " #" + k : o.get("message");
                    publish(c, topic, p);
                    Thread.sleep(interval);
                }
                Thread.sleep(500);
                System.exit(0);
            }
            System.out.println("[java] scrie mesaje (Enter = trimite). Prefix „@topic ” pentru alt subiect. Ctrl+D = ieșire.");
            BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            for (String line; (line = stdin.readLine()) != null; ) {
                if (line.isBlank()) continue;
                if (line.startsWith("@") && line.contains(" ")) {
                    topic = line.substring(1, line.indexOf(' '));
                    line = line.substring(line.indexOf(' ') + 1);
                }
                publish(c, topic, line);
            }
            Thread.sleep(300);
            System.exit(0);
        } else {
            for (String t : o.getOrDefault("topics", "#").split(",")) {
                c.send(msg("type", "subscribe", "topic", t.strip(), "id", UUID.randomUUID().toString()));
            }
            reader.join();
        }
    }

    static void publish(BrokerClient c, String topic, String payload) throws IOException {
        c.send(msg("type", "publish", "id", UUID.randomUUID().toString(), "topic", topic,
                "payload", payload, "checksum", sha256(payload), "timestamp", Instant.now().toString()));
    }
}
