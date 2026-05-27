package com.jagrosh.discordipc;

import com.jagrosh.discordipc.entities.*;
import com.jagrosh.discordipc.entities.Packet.OpCode;
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
import java.util.concurrent.atomic.AtomicBoolean;

public final class IPCClient implements Closeable
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(IPCClient.class);

    private final long clientId;

    private final HashMap<String, Callback> callbacks =
            new HashMap<>();

    private volatile Pipe pipe;

    private IPCListener listener = null;

    private Thread readThread = null;

    // FIX: proper lifecycle control
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public IPCClient(long clientId)
    {
        this.clientId = clientId;
    }

    public void setListener(IPCListener listener)
    {
        this.listener = listener;

        if(pipe != null)
        {
            pipe.setListener(listener);
        }
    }

    public synchronized void connect(DiscordBuild... preferredOrder)
            throws NoDiscordClientException
    {
        checkConnected(false);

        closed.set(false);

        callbacks.clear();

        pipe = null;

        pipe = Pipe.openPipe(
                this,
                clientId,
                callbacks,
                preferredOrder
        );

        LOGGER.debug("IPCClient connected");

        if(listener != null)
        {
            listener.onReady(this);
        }

        startReading();
    }

    public void sendRichPresence(RichPresence presence)
    {
        sendRichPresence(presence, null);
    }

    public void sendRichPresence(
            RichPresence presence,
            Callback callback
    ) {
        checkConnected(true);

        if(closed.get())
            return;

        try
        {
            LOGGER.debug(
                    "Sending RichPresence: {}",
                    presence == null
                            ? null
                            : presence.toJson().toString()
            );

            pipe.send(
                    OpCode.FRAME,
                    new JSONObject()
                            .put("cmd", "SET_ACTIVITY")
                            .put(
                                    "args",
                                    new JSONObject()
                                            .put("pid", getPID())
                                            .put(
                                                    "activity",
                                                    presence == null
                                                            ? null
                                                            : presence.toJson()
                                            )
                            ),
                    callback
            );
        }
        catch(Exception e)
        {
            LOGGER.error(
                    "Failed to send RichPresence",
                    e
            );
        }
    }

    public void subscribe(Event sub)
    {
        subscribe(sub, null);
    }

    public void subscribe(Event sub, Callback callback)
    {
        checkConnected(true);

        if(closed.get())
            return;

        if(!sub.isSubscribable())
        {
            throw new IllegalStateException(
                    "Cannot subscribe to " + sub + " event!"
            );
        }

        LOGGER.debug("Subscribing to event: {}", sub.name());

        pipe.send(
                OpCode.FRAME,
                new JSONObject()
                        .put("cmd", "SUBSCRIBE")
                        .put("evt", sub.name()),
                callback
        );
    }

    public PipeStatus getStatus()
    {
        if(pipe == null)
            return PipeStatus.UNINITIALIZED;

        return pipe.getStatus();
    }

    @Override
    public synchronized void close()
    {
        if(closed.compareAndSet(false, true))
        {
            LOGGER.info("Closing IPCClient");

            try
            {
                callbacks.clear();

                if(readThread != null)
                {
                    try
                    {
                        readThread.interrupt();
                    }
                    catch(Exception ignored) {}

                    readThread = null;
                }

                if(pipe != null)
                {
                    try
                    {
                        pipe.close();
                    }
                    catch(IOException e)
                    {
                        LOGGER.debug(
                                "Failed to close pipe",
                                e
                        );
                    }

                    pipe = null;
                }
            }
            catch(Exception e)
            {
                LOGGER.error(
                        "Exception while closing IPCClient",
                        e
                );
            }
        }
    }

    public DiscordBuild getDiscordBuild()
    {
        if(pipe == null)
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
                if(s != UNKNOWN
                        && s.name().equalsIgnoreCase(str))
                {
                    return s;
                }
            }

            return UNKNOWN;
        }
    }

    private void checkConnected(boolean connected)
    {
        if(connected
                && getStatus() != PipeStatus.CONNECTED)
        {
            throw new IllegalStateException(
                    String.format(
                            "IPCClient (ID: %d) is not connected!",
                            clientId
                    )
            );
        }

        if(!connected
                && getStatus() == PipeStatus.CONNECTED)
        {
            throw new IllegalStateException(
                    String.format(
                            "IPCClient (ID: %d) is already connected!",
                            clientId
                    )
            );
        }
    }

    private void startReading()
    {
        readThread = new Thread(() -> {

            try
            {
                Packet p;

                while(
                        !Thread.currentThread().isInterrupted()
                                && !closed.get()
                                && pipe != null
                                && pipe.getStatus() == PipeStatus.CONNECTED
                                && (p = pipe.read()).getOp() != OpCode.CLOSE
                ) {
                    JSONObject json = p.getJson();

                    Event event = Event.of(
                            json.optString("evt", null)
                    );

                    String nonce =
                            json.optString("nonce", null);

                    switch(event)
                    {
                        case NULL:

                            if(nonce != null
                                    && callbacks.containsKey(nonce))
                            {
                                callbacks.remove(nonce)
                                        .succeed(p);
                            }

                            break;

                        case ERROR:

                            if(nonce != null
                                    && callbacks.containsKey(nonce))
                            {
                                callbacks.remove(nonce)
                                        .fail(
                                                json.getJSONObject("data")
                                                        .optString(
                                                                "message",
                                                                null
                                                        )
                                        );
                            }

                            break;

                        case ACTIVITY_JOIN:
                            LOGGER.debug(
                                    "Received ACTIVITY_JOIN"
                            );
                            break;

                        case ACTIVITY_SPECTATE:
                            LOGGER.debug(
                                    "Received ACTIVITY_SPECTATE"
                            );
                            break;

                        case ACTIVITY_JOIN_REQUEST:
                            LOGGER.debug(
                                    "Received ACTIVITY_JOIN_REQUEST"
                            );
                            break;

                        case UNKNOWN:
                            LOGGER.debug(
                                    "Unknown event type: {}",
                                    json.optString("evt")
                            );
                            break;
                    }

                    if(listener != null
                            && json.has("cmd")
                            && json.getString("cmd")
                            .equals("DISPATCH"))
                    {
                        try
                        {
                            JSONObject data =
                                    json.getJSONObject("data");

                            switch(Event.of(
                                    json.getString("evt")
                            )) {
                                case ACTIVITY_JOIN:

                                    listener.onActivityJoin(
                                            this,
                                            data.getString("secret")
                                    );

                                    break;

                                case ACTIVITY_SPECTATE:

                                    listener.onActivitySpectate(
                                            this,
                                            data.getString("secret")
                                    );

                                    break;

                                case ACTIVITY_JOIN_REQUEST:

                                    JSONObject u =
                                            data.getJSONObject("user");

                                    User user = new User(
                                            u.getString("username"),
                                            u.getString("discriminator"),
                                            Long.parseLong(
                                                    u.getString("id")
                                            ),
                                            u.optString(
                                                    "avatar",
                                                    null
                                            )
                                    );

                                    listener.onActivityJoinRequest(
                                            this,
                                            data.optString(
                                                    "secret",
                                                    null
                                            ),
                                            user
                                    );

                                    break;
                            }
                        }
                        catch(Exception e)
                        {
                            LOGGER.error(
                                    "Exception while handling event",
                                    e
                            );
                        }
                    }
                }

                LOGGER.info("IPC read thread stopped");

                if(pipe != null)
                {
                    pipe.setStatus(
                            PipeStatus.DISCONNECTED
                    );
                }
            }
            catch(IOException | JSONException ex)
            {
                if(!closed.get())
                {
                    LOGGER.error(
                            "IPC read thread crashed",
                            ex
                    );

                    if(listener != null)
                    {
                        listener.onDisconnect(
                                this,
                                ex
                        );
                    }
                }
            }
            finally
            {
                if(pipe != null)
                {
                    pipe.setStatus(
                            PipeStatus.DISCONNECTED
                    );
                }

                if(listener != null && !closed.get())
                {
                    try
                    {
                        listener.onClose(
                                this,
                                null
                        );
                    }
                    catch(Exception ignored) {}
                }
            }

        }, "Discord-IPC-Read-Thread");

        // FIX: daemon thread
        readThread.setDaemon(true);

        LOGGER.debug("Starting IPC read thread");

        readThread.start();
    }

    private static int getPID()
    {
        String pr =
                ManagementFactory
                        .getRuntimeMXBean()
                        .getName();

        return Integer.parseInt(
                pr.substring(0, pr.indexOf('@'))
        );
    }
}
