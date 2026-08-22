package com.ferricstore;

import java.util.List;

public final class GeoStore {
    private final FerricStoreClient client;

    GeoStore(FerricStoreClient client) {
        this.client = client;
    }

    public long geoadd(String key, List<GeoMember> members) {
        return geoadd(key, members, GeoAddOptions.builder().build());
    }

    public long geoadd(String key, List<GeoMember> members, GeoAddOptions options) {
        if (members == null || members.isEmpty()) {
            throw new IllegalArgumentException("GEOADD requires at least one member");
        }
        java.util.Objects.requireNonNull(options, "GEOADD options");
        List<Object> args = CommandArgs.args("GEOADD", key);
        if (options.nx()) {
            args.add("NX");
        }
        if (options.xx()) {
            args.add("XX");
        }
        if (options.ch()) {
            args.add("CH");
        }
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

    public String geodist(String key, Object member1, Object member2, String unit) {
        List<Object> args =
                CommandArgs.args(
                        "GEODIST",
                        key,
                        client.codec().encode(member1),
                        client.codec().encode(member2));
        if (unit != null) {
            args.add(unit);
        }
        Object response = client.command(args);
        return response == null ? null : Resp.string(response);
    }

    public List<Object> geohash(String key, Object... members) {
        List<Object> args = CommandArgs.args("GEOHASH", key);
        for (Object member : members) {
            args.add(client.codec().encode(member));
        }
        return Resp.list(client.command(args));
    }

    public Object geosearch(String key, Object... options) {
        List<Object> args = CommandArgs.args("GEOSEARCH", key);
        args.addAll(List.of(options));
        return client.command(args);
    }

    public long geosearchstore(String destination, String source, Object... options) {
        HashSlot.requireSame("GEOSEARCHSTORE", List.of(destination, source));
        List<Object> args = CommandArgs.args("GEOSEARCHSTORE", destination, source);
        args.addAll(List.of(options));
        return Resp.number(client.command(args));
    }
}
