package com.jagrosh.discordipc.entities;

import java.time.OffsetDateTime;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Rich Presence object with buttons support.
 */
public class RichPresence
{
    private final String state;
    private final String details;
    private final OffsetDateTime startTimestamp;
    private final OffsetDateTime endTimestamp;

    private final String largeImageKey;
    private final String largeImageText;

    private final String smallImageKey;
    private final String smallImageText;

    private final String partyId;
    private final int partySize;
    private final int partyMax;

    private final String matchSecret;
    private final String joinSecret;
    private final String spectateSecret;

    private final boolean instance;

    // buttons
    private final JSONArray buttons;

    public RichPresence(
            String state,
            String details,
            OffsetDateTime startTimestamp,
            OffsetDateTime endTimestamp,
            String largeImageKey,
            String largeImageText,
            String smallImageKey,
            String smallImageText,
            String partyId,
            int partySize,
            int partyMax,
            String matchSecret,
            String joinSecret,
            String spectateSecret,
            boolean instance,
            JSONArray buttons)
    {
        this.state = state;
        this.details = details;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;

        this.largeImageKey = largeImageKey;
        this.largeImageText = largeImageText;

        this.smallImageKey = smallImageKey;
        this.smallImageText = smallImageText;

        this.partyId = partyId;
        this.partySize = partySize;
        this.partyMax = partyMax;

        this.matchSecret = matchSecret;
        this.joinSecret = joinSecret;
        this.spectateSecret = spectateSecret;

        this.instance = instance;

        this.buttons = buttons;
    }

    /**
     * Converts Rich Presence into JSON payload.
     */
    public JSONObject toJson()
    {
        JSONObject json = new JSONObject();

        if(state != null)
            json.put("state", state);

        if(details != null)
            json.put("details", details);

        // timestamps
        JSONObject timestamps = new JSONObject();

        if(startTimestamp != null)
            timestamps.put("start", startTimestamp.toEpochSecond());

        if(endTimestamp != null)
            timestamps.put("end", endTimestamp.toEpochSecond());

        if(timestamps.length() > 0)
            json.put("timestamps", timestamps);

        // assets
        JSONObject assets = new JSONObject();

        if(largeImageKey != null)
            assets.put("large_image", largeImageKey);

        if(largeImageText != null)
            assets.put("large_text", largeImageText);

        if(smallImageKey != null)
            assets.put("small_image", smallImageKey);

        if(smallImageText != null)
            assets.put("small_text", smallImageText);

        if(assets.length() > 0)
            json.put("assets", assets);

        // party
        if(partyId != null)
        {
            json.put("party",
                    new JSONObject()
                            .put("id", partyId)
                            .put("size", new JSONArray()
                                    .put(partySize)
                                    .put(partyMax)));
        }

        // secrets
        JSONObject secrets = new JSONObject();

        if(joinSecret != null)
            secrets.put("join", joinSecret);

        if(spectateSecret != null)
            secrets.put("spectate", spectateSecret);

        if(matchSecret != null)
            secrets.put("match", matchSecret);

        if(secrets.length() > 0)
            json.put("secrets", secrets);

        json.put("instance", instance);

        // buttons
        if(buttons != null && buttons.length() > 0)
        {
            json.put("buttons", buttons);
        }

        return json;
    }

    public static class Builder
    {
        private String state;
        private String details;

        private OffsetDateTime startTimestamp;
        private OffsetDateTime endTimestamp;

        private String largeImageKey;
        private String largeImageText;

        private String smallImageKey;
        private String smallImageText;

        private String partyId;
        private int partySize;
        private int partyMax;

        private String matchSecret;
        private String joinSecret;
        private String spectateSecret;

        private boolean instance;

        private JSONArray buttons;

        public Builder()
        {
            buttons = new JSONArray();
        }

        public RichPresence build()
        {
            return new RichPresence(
                    state,
                    details,
                    startTimestamp,
                    endTimestamp,
                    largeImageKey,
                    largeImageText,
                    smallImageKey,
                    smallImageText,
                    partyId,
                    partySize,
                    partyMax,
                    matchSecret,
                    joinSecret,
                    spectateSecret,
                    instance,
                    buttons
            );
        }

        public Builder setState(String state)
        {
            this.state = state;
            return this;
        }

        public Builder setDetails(String details)
        {
            this.details = details;
            return this;
        }

        public Builder setStartTimestamp(OffsetDateTime startTimestamp)
        {
            this.startTimestamp = startTimestamp;
            return this;
        }

        public Builder setEndTimestamp(OffsetDateTime endTimestamp)
        {
            this.endTimestamp = endTimestamp;
            return this;
        }

        public Builder setLargeImage(String key, String text)
        {
            this.largeImageKey = key;
            this.largeImageText = text;
            return this;
        }

        public Builder setLargeImage(String key)
        {
            return setLargeImage(key, null);
        }

        public Builder setSmallImage(String key, String text)
        {
            this.smallImageKey = key;
            this.smallImageText = text;
            return this;
        }

        public Builder setSmallImage(String key)
        {
            return setSmallImage(key, null);
        }

        public Builder setParty(String id, int size, int max)
        {
            this.partyId = id;
            this.partySize = size;
            this.partyMax = max;
            return this;
        }

        public Builder setMatchSecret(String secret)
        {
            this.matchSecret = secret;
            return this;
        }

        public Builder setJoinSecret(String secret)
        {
            this.joinSecret = secret;
            return this;
        }

        public Builder setSpectateSecret(String secret)
        {
            this.spectateSecret = secret;
            return this;
        }

        public Builder setInstance(boolean instance)
        {
            this.instance = instance;
            return this;
        }

        /**
         * Add button to Rich Presence.
         * Discord supports maximum 2 buttons.
         */
        public Builder addButton(String label, String url)
        {
            if(buttons.length() >= 2)
                return this;

            JSONObject button = new JSONObject();

            button.put("label", label);
            button.put("url", url);

            buttons.put(button);

            return this;
        }
    }
}
