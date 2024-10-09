package com.roblox.trino.udfs;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TrinoUdfsQueryRunner
{
    private TrinoUdfsQueryRunner() {}

    public static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder().setSchema("tpch").build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setExtraProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            queryRunner.installPlugin(new TrinoUdfsPlugin());
            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createQueryRunner();
        Logger log = Logger.get(TrinoUdfsQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
