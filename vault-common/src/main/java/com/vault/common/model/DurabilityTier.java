package com.vault.common.model;

public enum DurabilityTier {
    CRITICAL(5, 3), 
    STANDARD(3, 2), 
    TEMPORARY(1, 1);

    private final int n;
    private final int w;

    DurabilityTier(int n, int w) {
        this.n = n;
        this.w = w;
    }

    public int getN() { return n; }
    public int getW() { return w; }
}
