package jp.co.rakuten.rit.roma.client.commands;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.Connection;
import jp.co.rakuten.rit.roma.client.command.CommandContext;
import jp.co.rakuten.rit.roma.client.command.CommandException;

public class GetsCommand extends DefaultCommand implements CommandID {
    @Override
    public boolean execute(CommandContext context) throws CommandException {
        try {
            // "gets <key>*\r\n"
            StringBuilder sb = new StringBuilder();
            List<String> keys = (List<String>) context.get(CommandContext.KEYS);

            sb.append(STR_GETS);
            for (Iterator<String> iter = keys.iterator(); iter.hasNext(); ) {
            	sb.append(STR_WHITE_SPACE).append(iter.next());
            }
            sb.append(STR_CRLF);

            Connection conn = (Connection) context.get(CommandContext.CONNECTION);
            conn.out.write(sb.toString().getBytes());
            conn.out.flush();

            // VALUE foo 0 <valueLen>\r\n<value>\r\n or END\r\n
            Map<String, byte[]> values = null;
            String s;
            s = conn.in.readLine();
            if (s.startsWith("VALUE")) {
                values = new HashMap<String, byte[]>();
            } else if (s.startsWith("END")) {
                return false;
            } else if (s.startsWith("ERROR")) {
                //throw new UnsupportedOperationException("Not supported yet.");
                throw new CommandException("Not supported yet.");
            } else if (s.startsWith("SERVER_ERROR")) {
                throw new CommandException(s);
            } else if (s.startsWith("CLIENT_ERROR")) {
                throw new CommandException(s);
            } else {
                //throw new UnsupportedOperationException("Not supported yet.");
                throw new CommandException("Not supported yet.");
            }

            do {
                StringTokenizer t = new StringTokenizer(s);
                t.nextToken(); // VALUE
                String key = t.nextToken(); // key
                t.nextToken(); // 0
                int valueLen = Integer.parseInt(t.nextToken()); // len
                // TODO cas ID t.nextToken();

                // value
                byte[] value = new byte[valueLen];
                int offset = 0;
                int size = 0;
                while (offset < valueLen) {
                    size = conn.in.read(value, offset, valueLen - offset);
                    offset = offset + size;
                }
                conn.in.read(2); // "\r\n"
                values.put(key, value);

            	s = conn.in.readLine();
            } while (!s.equals("END"));

            context.put(CommandContext.RESULT, values);
            return true;
        } catch (IOException e) {
            throw new CommandException(e);
        }
    }

    @Override
    protected void sendAndReceive(CommandContext context) throws IOException,
            ClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void create(CommandContext context) throws BadCommandException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean parseResult(CommandContext context)
            throws ClientException {
        throw new UnsupportedOperationException();
    }
}