package com.vault.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WriteResult {
    private String checksumSha256;
    private long sizeBytes;
}
