package com.jagrosh.discordipc;

import com.jagrosh.discordipc.entities.Callback;
import com.jagrosh.discordipc.entities.DiscordBuild;
import com.jagrosh.discordipc.entities.Packet;
import com.jagrosh.discordipc.entities.RichPresence;
import com.jagrosh.discordipc.entities.User;
import com.jagrosh.discordipc.entities.pipe.Pipe;
import com.jagrosh.discordipc.entities.pipe.PipeStatus;
import com.jagrosh.discordipc.exceptions.NoDiscordClientException;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;

public final class IPCClient implements Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IPCClient.class);

    private final long clientId;
    private final HashMap<String, Callback> callbacks = new HashMap<>();

    private volatile Pipe pipe;
    private IPCListener listener = null;
    private Thread readThread = null;

    public IPCClient(long clientId)
    {
        this.clientId = clientId;
    }

    public void setListener(IPCListener listener)
    {
        this.listener = listener;

        if (pipe != null)
            pipe.setListener(listener);
    }

    public void connect(DiscordBuild... preferredOrder) throws NoDiscordClientException
    {
        checkConnected(false);

        callbacks.clear();
        pipe = null;

        pipe = Pipe.openPipe(this, clientId, callbacks, preferredOrder);

        LOGGER.info("IPCClient connected");

        if(listener != null)
            listener.onReady(this);

        startReading();
    }

    public void sendRichPresence(RichPresence presence)
    {
        sendRichPresence(presence, null);
    }

    public void sendRichPresence(RichPresence presence, Callback callback)
    {
        checkConnected(true);

        LOGGER.debug("Sending RichPresence");

        pipe.send(
                Packet.OpCode.FRAME,
                new JSONObject()
                        .put("cmd", "SET_ACTIVITY")
                        .put("args", new JSONObject()
                                .put("pid", getPID())
                                .put("activity", presence == null ? null : presence.toJson())),
                callback
        );
    }

    public void subscribe(Event sub)
    {
        subscribe(sub, null);
    }

    public void subscribe(Event sub, Callback callback)
    {
        checkConnected(true);

        if(!sub.isSubscribable())
            throw new IllegalStateException("Cannot subscribe to " + sub + " event!");

        pipe.send(
                Packet.OpCode.FRAME,
                new JSONObject()
                        .put("cmd", "SUBSCRIBE")
                        .put("evt", sub.name()),
                callback
        );
    }

    public PipeStatus getStatus()
    {
        if (pipe == null)
            return PipeStatus.UNINITIALIZED;

        return pipe.getStatus();
    }

    @Override
    public void close()
    {
        LOGGER.info("Closing IPCClient");

        try
        {
            if (pipe != null)
            {
                pipe.setStatus(PipeStatus.CLOSED);
                pipe.close();
            }
        }
        catch (Exception e)
        {
            LOGGER.error("Failed to close IPC pipe", e);
        }

        pipe = null;

        if (readThread != null)
        {
            readThread.interrupt();
            readThread = null;
        }

        callbacks.clear();
    }

    public DiscordBuild getDiscordBuild()
    {
        if (pipe == null)
            return null;

        return pipe.getDiscordBuild();
    }

    public enum Event
    {
        NULL(false),
        READY(false),
        ERROR(false),
        ACTIVITY_JOIN(true),
        ACTIVITY_SPECTATE(true),
        ACTIVITY_JOIN_REQUEST(true),
        UNKNOWN(false);

        private final boolean subscribable;

        Event(boolean subscribable)
        {
            this.subscribable = subscribable;
        }

        public boolean isSubscribable()
        {
            return subscribable;
        }

        static Event of(String str)
        {
            if(str == null)
                return NULL;

            for(Event s : Event.values())
            {
                if(s != UNKNOWN && s.name().equalsIgnoreCase(str))
                    return s;
            }

            return UNKNOWN;
        }
    }

    private void checkConnected(boolean connected)
    {
        if(connected && getStatus() != PipeStatus.CONNECTED)
            throw new IllegalStateException(
                    String.format("IPCClient (ID: %d) is not connected!", clientId)
            );

        if(!connected && getStatus() == PipeStatus.CONNECTED)
            throw new IllegalStateException(
                    String.format("IPCClient (ID: %d) is already connected!", clientId)
            );
    }

    private void startReading()
    {
        readThread = new Thread(() -> {

            try
            {
                while (pipe != null && pipe.getStatus() == PipeStatus.CONNECTED)
                {
                    Packet p;

                    try
                    {
                        p = pipe.read();
                    }
                    catch (IOException ex)
                    {
                        break;
                    }

                    if (p == null)
                        continue;

                    if (p.getOp() == Packet.OpCode.CLOSE)
                        break;

                    JSONObject json = p.getJson();

                    if (json == null)
                        continue;

                    Event event = Event.of(json.optString("evt", null));
                    String nonce = json.optString("nonce", null);

                    switch(event)
                    {
                        case NULL:
                            if(nonce != null && callbacks.containsKey(nonce))
                                callbacks.remove(nonce).succeed(p);
                            break;

                        case ERROR:
                            if(nonce != null && callbacks.containsKey(nonce))
                            {
                                callbacks.remove(nonce).fail(
                                        json.optJSONObject("data") != null
                                                ? json.getJSONObject("data").optString("message", null)
                                                : "Unknown error"
                                );
                            }
                            break;

                        case ACTIVITY_JOIN:
                            LOGGER.debug("Join event");
                            break;

                        case ACTIVITY_SPECTATE:
                            LOGGER.debug("Spectate event");
                            break;

                        case ACTIVITY_JOIN_REQUEST:
                            LOGGER.debug("Join request event");
                            break;

                        case UNKNOWN:
                            LOGGER.debug("Unknown event");
                            break;
                    }

                    if(listener != null
                            && json.has("cmd")
                            && "DISPATCH".equals(json.optString("cmd")))
                    {
                        try
                        {
                            JSONObject data = json.optJSONObject("data");

                            if(data == null)
                                continue;

                            switch(Event.of(json.optString("evt")))
                            {
                                case ACTIVITY_JOIN:
                                    listener.onActivityJoin(this, data.getString("secret"));
                                    break;

                                case ACTIVITY_SPECTATE:
                                    listener.onActivitySpectate(this, data.getString("secret"));
                                    break;

                                case ACTIVITY_JOIN_REQUEST:

                                    JSONObject u = data.getJSONObject("user");

                                    User user = new User(
                                            u.getString("username"),
                                            u.getString("discriminator"),
                                            Long.parseLong(u.getString("id")),
                                            u.optString("avatar", null)
                                    );

                                    listener.onActivityJoinRequest(
                                            this,
                                            data.optString("secret", null),
                                            user
                                    );

                                    break;
                            }
                        }
                        catch(Exception e)
                        {
                            LOGGER.error("Event error", e);
                        }
                    }
                }
            }
            catch(IOException | JSONException ex)
            {
                LOGGER.error("Reading thread crashed", ex);

                if(listener != null)
                    listener.onDisconnect(this, ex);
            }
            catch(Exception ex)
            {
                LOGGER.error("Unexpected IPC error", ex);
            }
            finally
            {
                try
                {
                    if (pipe != null)
                    {
                        pipe.setStatus(PipeStatus.DISCONNECTED);
                        pipe.close();
                    }
                }
                catch(Exception ignored) {}

                LOGGER.info("IPC reading thread stopped");
            }
        });

        readThread.setName("Discord-IPC-ReadThread");

        // КРИТИЧНО
        readThread.setDaemon(true);

        LOGGER.debug("Starting IPCClient reading thread");

        readThread.start();
    }

    private static int getPID()
    {
        String pr = ManagementFactory.getRuntimeMXBean().getName();
        return Integer.parseInt(pr.substring(0, pr.indexOf('@')));
    }
}
