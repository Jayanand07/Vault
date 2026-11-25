package com.vault.node.repository;

import com.vault.common.model.DurabilityTier;
import com.vault.common.model.ObjectStatus;
import com.vault.common.util.ObjectKey;
import com.vault.node.TestEphemeralCredentials;
import com.vault.node.model.ObjectMeta;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.ContextConfiguration;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression test for V-21: SQL LIKE wildcard escaping (_, %, \) in prefix listings.
 */
@DataJpaTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:vault_wildcard_test;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.flyway.enabled=false",
        "spring.jpa.hibernate.ddl-auto=create-drop"
})
@ContextConfiguration(initializers = TestEphemeralCredentials.class)
class ObjectMetaRepositoryWildcardTest {

    @Autowired
    private ObjectMetaRepository repository;

    @Test
    void prefixListingEscapesUnderscoreWildcardLiterally() {
        saveActive("a_b/file1.txt");
        saveActive("axb/file2.txt");

        String escaped = ObjectKey.escapeLikePrefix("a_b");
        Page<ObjectMeta> matches = repository.findByObjectIdStartingWithAndStatusOnlyLatest(
                escaped, ObjectStatus.ACTIVE, PageRequest.of(0, 50));

        assertEquals(1, matches.getTotalElements(),
                "Prefix 'a_b' must not match 'axb/file2.txt' via unescaped SQL underscore wildcard");
        assertEquals("a_b/file1.txt", matches.getContent().get(0).getObjectId());
    }

    private void saveActive(String objectId) {
        ObjectMeta meta = new ObjectMeta();
        meta.setObjectId(objectId);
        meta.setVersion(1L);
        meta.setSizeBytes(4L);
        meta.setChecksumSha256("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        meta.setDurabilityTier(DurabilityTier.STANDARD);
        meta.setTerm(1L);
        meta.setStatus(ObjectStatus.ACTIVE);
        repository.saveAndFlush(meta);
    }
}
