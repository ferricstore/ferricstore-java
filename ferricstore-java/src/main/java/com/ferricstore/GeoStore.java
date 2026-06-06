package com.ferricstore;

import java.util.List;

public final class GeoStore {
    private final FerricStoreClient client;

    GeoStore(FerricStoreClient client) {
        this.client = client;
    }

    public long geoadd(String key, List<GeoMember> members) {
        List<Object> args = CommandArgs.args("GEOADD", key);
        for (GeoMember member : members) {
            args.add(member.longitude());
            args.add(member.latitude());
            args.add(client.codec().encode(member.member()));
        }
        return Resp.number(client.command(args));
    }

    public Object geopos(String key, Object... members) {
        List<Object> args = CommandArgs.args("GEOPOS", key);
        for (Object member : members) {
            args.add(client.codec().encode(member));
        }
        return client.command(args);
    }
}
