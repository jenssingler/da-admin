/*
 * Copyright 2024 Adobe. All rights reserved.
 * This file is licensed to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
const jwtVerify = async (token, jwks) => {
  const { 0: email, 1: created = 0, 2: expires = 0, 3: valid = "true" } = token.split(':');
  const created_at = Math.floor(new Date().getTime() / 1000) + Number(created);
  const expires_in = Number(expires);
  if (valid !== 'true') {
    throw new Error('Validation failed');
  }
  return {
    payload:
      {
        user_id: email,
        created_at,
        expires_in: expires_in || created_at + 1000,
      }
  };
};

const createRemoteJWKSet = async () => {}

export default { jwtVerify, createRemoteJWKSet };
