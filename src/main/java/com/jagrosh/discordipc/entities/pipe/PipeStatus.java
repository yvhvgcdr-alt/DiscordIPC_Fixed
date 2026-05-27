package com.jagrosh.discordipc.entities.pipe;

import com.jagrosh.discordipc.IPCClient;
import com.jagrosh.discordipc.IPCListener;
import com.jagrosh.discordipc.entities.DiscordBuild;
import com.jagrosh.discordipc.entities.Packet;

public enum PipeStatus
{
    UNINITIALIZED,

    CONNECTING,

    CONNECTED,

    CLOSED,

    DISCONNECTED
}
