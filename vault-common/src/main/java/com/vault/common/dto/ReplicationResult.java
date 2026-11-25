package com.vault.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReplicationResult {
    private String objectId;
    private long version;
    private String checksumSha256;
    private long acks;
}
