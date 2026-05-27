package com.jagrosh.discordipc.entities.pipe;

import com.jagrosh.discordipc.IPCClient;
import com.jagrosh.discordipc.IPCListener;
import com.jagrosh.discordipc.entities.Callback;
import com.jagrosh.discordipc.entities.DiscordBuild;
import com.jagrosh.discordipc.entities.Packet;
import com.jagrosh.discordipc.exceptions.NoDiscordClientException;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Pipe {

    private static final Logger LOGGER = LoggerFactory.getLogger(Pipe.class);

    private static final int VERSION = 1;

    PipeStatus status = PipeStatus.CONNECTING;

    IPCListener listener;

    private DiscordBuild build;

    final IPCClient ipcClient;

    private final HashMap<String, Callback> callbacks;

    // FIX: prevents double-close / hanging shutdown
    private final AtomicBoolean closed = new AtomicBoolean(false);

    Pipe(IPCClient ipcClient, HashMap<String, Callback> callbacks)
    {
        this.ipcClient = ipcClient;
        this.callbacks = callbacks;
    }

    public static Pipe openPipe(
            IPCClient ipcClient,
            long clientId,
            HashMap<String,Callback> callbacks,
            DiscordBuild... preferredOrder
    ) throws NoDiscordClientException {

        if(preferredOrder == null || preferredOrder.length == 0)
            preferredOrder = new DiscordBuild[]{DiscordBuild.ANY};

        Pipe pipe = null;

        Pipe[] open = new Pipe[DiscordBuild.values().length];

        for(int i = 0; i < 10; i++)
        {
            try
            {
                String location = getPipeLocation(i);

                LOGGER.debug("Searching for IPC: {}", location);

                pipe = createPipe(ipcClient, callbacks, location);

                pipe.send(
                        Packet.OpCode.HANDSHAKE,
                        new JSONObject()
                                .put("v", VERSION)
                                .put("client_id", Long.toString(clientId)),
                        null
                );

                Packet p = pipe.read();

                pipe.build = DiscordBuild.from(
                        p.getJson()
                                .getJSONObject("data")
                                .getJSONObject("config")
                                .getString("api_endpoint")
                );

                LOGGER.debug(
                        "Found valid client ({}) with packet: {}",
                        pipe.build.name(),
                        p
                );

                if(pipe.build == preferredOrder[0]
                        || DiscordBuild.ANY == preferredOrder[0])
                {
                    LOGGER.info("Found preferred client: {}", pipe.build.name());
                    break;
                }

                open[pipe.build.ordinal()] = pipe;
                open[DiscordBuild.ANY.ordinal()] = pipe;

                pipe.build = null;
                pipe = null;
            }
            catch(IOException | JSONException ex)
            {
                LOGGER.debug("Pipe open failed", ex);

                if(pipe != null)
                {
                    try {
                        pipe.close();
                    } catch(Exception ignored) {}
                }

                pipe = null;
            }
        }

        if(pipe == null)
        {
            for(int i = 1; i < preferredOrder.length; i++)
            {
                DiscordBuild cb = preferredOrder[i];

                LOGGER.debug("Looking for client build: {}", cb.name());

                if(open[cb.ordinal()] != null)
                {
                    pipe = open[cb.ordinal()];

                    open[cb.ordinal()] = null;

                    if(cb == DiscordBuild.ANY)
                    {
                        for(int k = 0; k < open.length; k++)
                        {
                            if(open[k] == pipe)
                            {
                                pipe.build = DiscordBuild.values()[k];
                                open[k] = null;
                            }
                        }
                    }
                    else
                    {
                        pipe.build = cb;
                    }

                    LOGGER.info("Found preferred client: {}", pipe.build.name());

                    break;
                }
            }

            if(pipe == null)
            {
                throw new NoDiscordClientException();
            }
        }

        // FIX: properly close unused pipes
        for(int i = 0; i < open.length; i++)
        {
            if(i == DiscordBuild.ANY.ordinal())
                continue;

            if(open[i] != null)
            {
                try
                {
                    open[i].close();
                }
                catch(IOException ex)
                {
                    LOGGER.debug("Failed to close unused IPC pipe", ex);
                }
            }
        }

        pipe.status = PipeStatus.CONNECTED;

        return pipe;
    }

    private static Pipe createPipe(
            IPCClient ipcClient,
            HashMap<String, Callback> callbacks,
            String location
    ) {
        String osName = System.getProperty("os.name").toLowerCase();

        if (osName.contains("win"))
        {
            return new WindowsPipe(ipcClient, callbacks, location);
        }
        else if (osName.contains("linux") || osName.contains("mac"))
        {
            try
            {
                return new UnixPipe(ipcClient, callbacks, location);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        else
        {
            throw new RuntimeException("Unsupported OS: " + osName);
        }
    }

    public void send(Packet.OpCode op, JSONObject data, Callback callback)
    {
        if(isClosed())
            return;

        try
        {
            String nonce = generateNonce();

            Packet p = new Packet(
                    op,
                    data.put("nonce", nonce)
            );

            if(callback != null && !callback.isEmpty())
            {
                callbacks.put(nonce, callback);
            }

            write(p.toBytes());

            LOGGER.debug("Sent packet: {}", p);

            if(listener != null)
            {
                listener.onPacketSent(ipcClient, p);
            }
        }
        catch(IOException ex)
        {
            LOGGER.error(
                    "IOException while sending packet; disconnecting",
                    ex
            );

            status = PipeStatus.DISCONNECTED;

            try {
                close();
            } catch(Exception ignored) {}
        }
    }

    public abstract Packet read() throws IOException, JSONException;

    public abstract void write(byte[] b) throws IOException;

    private static String generateNonce()
    {
        return UUID.randomUUID().toString();
    }

    public PipeStatus getStatus()
    {
        return status;
    }

    public void setStatus(PipeStatus status)
    {
        this.status = status;
    }

    public void setListener(IPCListener listener)
    {
        this.listener = listener;
    }

    // FIX: unified safe close
    public final void close() throws IOException
    {
        if(closed.compareAndSet(false, true))
        {
            LOGGER.info("Closing IPC pipe");

            status = PipeStatus.DISCONNECTED;

            callbacks.clear();

            try
            {
                closePipe();
            }
            finally
            {
                listener = null;
            }
        }
    }

    // actual implementation
    protected abstract void closePipe() throws IOException;

    public boolean isClosed()
    {
        return closed.get();
    }

    public DiscordBuild getDiscordBuild()
    {
        return build;
    }

    private final static String[] unixPaths = {
            "XDG_RUNTIME_DIR",
            "TMPDIR",
            "TMP",
            "TEMP"
    };

    private static String getPipeLocation(int i)
    {
        if(System.getProperty("os.name").contains("Win"))
            return "\\\\?\\pipe\\discord-ipc-" + i;

        String tmppath = null;

        for(String str : unixPaths)
        {
            tmppath = System.getenv(str);

            if(tmppath != null)
                break;
        }

        if(tmppath == null)
            tmppath = "/tmp";

        return tmppath + "/discord-ipc-" + i;
    }
}
