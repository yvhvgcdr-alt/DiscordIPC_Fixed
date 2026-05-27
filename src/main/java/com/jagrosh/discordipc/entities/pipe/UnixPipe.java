package com.jagrosh.discordipc.entities.pipe;

import com.jagrosh.discordipc.IPCClient;
import com.jagrosh.discordipc.entities.Callback;
import com.jagrosh.discordipc.entities.Packet;

import org.json.JSONException;
import org.json.JSONObject;

import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

import java.nio.ByteBuffer;

import java.nio.file.Paths;

import java.util.HashMap;

public class UnixPipe extends Pipe
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(UnixPipe.class);

    private final AFUNIXSocket socket;

    UnixPipe(
            IPCClient ipcClient,
            HashMap<String, Callback> callbacks,
            String location
    ) throws IOException {

        super(ipcClient, callbacks);

        socket = AFUNIXSocket.newInstance();

        socket.connect(
                AFUNIXSocketAddress.of(
                        Paths.get(location)
                )
        );
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public Packet read() throws IOException, JSONException
    {
        InputStream is = socket.getInputStream();

        while(
                is.available() == 0
                        && status == PipeStatus.CONNECTED
                        && !Thread.currentThread().isInterrupted()
        ) {
            try
            {
                Thread.sleep(50);
            }
            catch(InterruptedException e)
            {
                Thread.currentThread().interrupt();

                throw new IOException(
                        "IPC read thread interrupted",
                        e
                );
            }
        }

        if(status == PipeStatus.DISCONNECTED)
        {
            throw new IOException("Disconnected!");
        }

        if(status == PipeStatus.CLOSED)
        {
            return new Packet(
                    Packet.OpCode.CLOSE,
                    null
            );
        }

        byte[] d = new byte[8];

        is.read(d);

        ByteBuffer bb = ByteBuffer.wrap(d);

        Packet.OpCode op =
                Packet.OpCode.values()[
                        Integer.reverseBytes(
                                bb.getInt()
                        )
                ];

        d = new byte[
                Integer.reverseBytes(
                        bb.getInt()
                )
        ];

        is.read(d);

        Packet p = new Packet(
                op,
                new JSONObject(new String(d))
        );

        LOGGER.debug(
                "Received packet: {}",
                p
        );

        if(listener != null)
        {
            listener.onPacketReceived(
                    ipcClient,
                    p
            );
        }

        return p;
    }

    @Override
    public void write(byte[] b) throws IOException
    {
        if(status == PipeStatus.CLOSED)
        {
            throw new IOException(
                    "Pipe is closed!"
            );
        }

        socket.getOutputStream().write(b);

        socket.getOutputStream().flush();
    }

    @Override
    protected void closePipe() throws IOException
    {
        LOGGER.debug("Closing IPC pipe...");

        try
        {
            if(status == PipeStatus.CONNECTED)
            {
                send(
                        Packet.OpCode.CLOSE,
                        new JSONObject(),
                        null
                );
            }
        }
        catch(Exception ignored) {}

        status = PipeStatus.CLOSED;

        try
        {
            socket.close();
        }
        catch(Exception ignored) {}
    }
}
