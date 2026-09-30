import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '30s', target: 20 },  // Ramp-up to 20 users
    { duration: '1m', target: 20 },   // Stay at 20 users for 1 min
    { duration: '30s', target: 0 },   // Ramp-down to 0 users
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'], // 95% of requests must be under 500ms
  },
};

const BASE_URL = 'http://localhost:8081/api/v1/objects';
// If using API keys, configure them here
const API_KEY = ''; 

export default function () {
  const objectId = `load-test-obj-${__VU}-${__ITER}`;
  const payload = 'A'.repeat(1024); // 1KB payload

  const params = {
    headers: {
      'Content-Type': 'application/octet-stream',
      'X-Vault-Durability': 'STANDARD',
      ...(API_KEY && { 'X-API-Key': API_KEY })
    },
  };

  // 1. Write the object
  let putRes = http.put(`${BASE_URL}/${objectId}`, payload, params);
  check(putRes, {
    'write status is 201': (r) => r.status === 201,
  });

  sleep(0.5);

  // 2. Read the object
  let getRes = http.get(`${BASE_URL}/${objectId}`, params);
  check(getRes, {
    'read status is 200': (r) => r.status === 200,
    'read length matches': (r) => r.body.length === 1024,
  });

  sleep(1);
}
