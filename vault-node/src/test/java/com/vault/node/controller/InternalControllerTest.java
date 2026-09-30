package com.vault.node.controller;

import com.vault.common.util.ChecksumUtil;
import com.vault.node.election.ElectionService;
import com.vault.node.election.NodeState;
import com.vault.node.repository.ReplicaRepository;
import com.vault.node.service.LocalStorageService;
import com.vault.node.service.NodeRegistryService;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InternalControllerTest {

    @Mock
    private LocalStorageService localStorageService;
    
    @Mock
    private ReplicaRepository replicaRepository;
    
    @Mock
    private NodeRegistryService nodeRegistryService;
    
    @Mock
    private ElectionService electionService;
    
    @Mock
    private NodeState nodeState;
    
    @Mock
    private HttpServletRequest request;

    @InjectMocks
    private InternalController controller;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(controller, "nodeSecret", "secret123");
    }

    @Test
    void testReplicateComputesCorrectChecksum() throws Exception {
        byte[] data = "test stream data".getBytes();
        String expectedChecksum = ChecksumUtil.sha256(data);
        
        when(nodeState.getCurrentTerm()).thenReturn(1L);
        
        ServletInputStream servletInputStream = new ServletInputStream() {
            private final ByteArrayInputStream bais = new ByteArrayInputStream(data);
            
            @Override
            public boolean isFinished() { return bais.available() == 0; }
            @Override
            public boolean isReady() { return true; }
            @Override
            public void setReadListener(ReadListener readListener) {}
            @Override
            public int read() throws IOException { return bais.read(); }
            @Override
            public int read(byte[] b, int off, int len) throws IOException { return bais.read(b, off, len); }
        };
        
        when(request.getInputStream()).thenReturn(servletInputStream);
        when(nodeRegistryService.getSelfNodeId()).thenReturn(java.util.UUID.randomUUID());

        ResponseEntity<?> response = controller.replicate(
                "secret123", expectedChecksum, 1L, 1L, "obj-1", request);
                
        assertEquals(HttpStatus.OK, response.getStatusCode());
        
        // If checksum was wrong, it would return HttpStatus.BAD_REQUEST
        verify(localStorageService, times(1)).store(eq("obj-1"), eq(1L), any());
        verify(replicaRepository, times(1)).save(any());
    }

    @Test
    void testReplicateRejectsMismatchedChecksum() throws Exception {
        byte[] data = "test stream data".getBytes();
        String mismatchedChecksum = "wronghash123";
        
        when(nodeState.getCurrentTerm()).thenReturn(1L);
        
        ServletInputStream servletInputStream = new ServletInputStream() {
            private final ByteArrayInputStream bais = new ByteArrayInputStream(data);
            
            @Override
            public boolean isFinished() { return bais.available() == 0; }
            @Override
            public boolean isReady() { return true; }
            @Override
            public void setReadListener(ReadListener readListener) {}
            @Override
            public int read() throws IOException { return bais.read(); }
            @Override
            public int read(byte[] b, int off, int len) throws IOException { return bais.read(b, off, len); }
        };
        
        when(request.getInputStream()).thenReturn(servletInputStream);
        
        // nodeRegistryService.getSelfNodeId() is only called on SUCCESS path in InternalController
        // so we don't need to mock it here, because it will return early.

        ResponseEntity<?> response = controller.replicate(
                "secret123", mismatchedChecksum, 1L, 1L, "obj-2", request);
                
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        
        // Verify we didn't save anything
        verify(localStorageService, never()).store(anyString(), anyLong(), any());
        verify(replicaRepository, never()).save(any());
    }
}
