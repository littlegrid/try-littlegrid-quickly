package org.littlegrid.tryquickly;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.DistributedCacheService;
import com.tangosol.net.NamedCache;
import com.tangosol.net.cache.TypeAssertion;
import org.junit.Before;
import org.junit.Test;
import org.littlegrid.ClusterMemberGroupUtils;
import org.littlegrid.impl.SimpleKeepAliveClusterMemberGroup;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class ExamplePartialStackIntegrationTest {
    private static final int STORAGE_ENABLED_MEMBERS = 2;
    private NamedCache<Integer, String> cache;

    @Before
    public void beforeEachTest() {
        ClusterMemberGroupUtils.newBuilder()
                .setStorageEnabledCount(STORAGE_ENABLED_MEMBERS)
                .setClusterMemberGroupInstanceClassName(SimpleKeepAliveClusterMemberGroup.class.getName())
                .setFastStartJoinTimeoutMilliseconds(100)
                .buildAndConfigureForStorageDisabledClient();

        cache = CacheFactory.getTypedCache("example", TypeAssertion.withTypes(Integer.class, String.class));
        cache.clear();
    }

    @Test
    public void simpleExample1() {
        cache.put(1, "hello");

        assertThat(cache.size(), equalTo(1));
    }

    @Test
    public void simpleExample2() {
        assertThat(CacheFactory.getCluster().getMemberSet().size(), equalTo(STORAGE_ENABLED_MEMBERS + 1));
    }

    @Test
    public void simpleExample3() {
        DistributedCacheService partitionedCacheService = (DistributedCacheService) cache.getCacheService();

        assertThat(partitionedCacheService.getOwnershipEnabledMembers().size(), equalTo(STORAGE_ENABLED_MEMBERS));
    }
}
