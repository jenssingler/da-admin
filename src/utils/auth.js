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
import { createRemoteJWKSet, jwtVerify } from 'jose';

const JWKS_IMS = await createRemoteJWKSet(new URL('https://ims-na1.adobelogin.com/ims/keys'));

export async function setUser(userId, expiration, headers, env) {
  const resp = await fetch(`${env.IMS_ORIGIN}/ims/profile/v1`, { headers });
  if (!resp.ok) {
    // Something went wrong - either with the connection or the token isn't valid
    // assume we are anon for now (but don't cache so we can try again next time)
    return null;
  }
  const json = await resp.json();

  const value = JSON.stringify({ email: json.email });
  await env.DA_AUTH.put(userId, value, { expiration });
  return value;
}

export async function getUsers(req, env) {
  const authHeader = req.headers?.get('authorization');
  if (!authHeader) return [{ email: 'anonymous' }];

  async function parseUser(token) {
    if (!token || token.trim().length === 0) return { email: 'anonymous' };

    let expires;
    let userId;
    try {
      const { payload } = await jwtVerify(token, JWKS_IMS);
      const { created_at: createdAt, expires_in: expiresIn } = payload;
      userId = payload.user_id;
      expires = Number(createdAt) + Number(expiresIn);
    } catch {
      return { email: 'anonymous' };
    }

    const now = Math.floor(new Date().getTime() / 1000);

    if (expires < now) return { email: 'anonymous' };
    // Find the user in recent sessions
    let user = await env.DA_AUTH.get(userId);

    // If not found, add them to recent sessions
    if (!user) {
      const headers = new Headers(req.headers);
      headers.delete('authorization');
      headers.set('authorization', `Bearer ${token}`);
      // If not found, create them
      user = await setUser(userId, Math.floor(expires / 1000), headers, env);
    }

    // If there's still no user, make them anon.
    if (!user) return { email: 'anonymous' };

    // Finally, return whoever was made.
    return JSON.parse(user);
  }

  return Promise.all(
    authHeader.split(',')
      .map((auth) => auth.split(' ').pop())
      .map(parseUser),
  );
}

export async function isAuthorized(env, org, user) {
  if (!org) return true;

  let props = await env.DA_CONFIG.get(org, { type: 'json' });
  if (!props) return true;

  // When the data is a multi-sheet, it's one level deeper
  if (props[':type'] === 'multi-sheet') {
    props = props.data;
  }

  const admins = props.data.reduce((acc, data) => {
    if (data.key === 'admin.role.all') acc.push(data.value);
    return acc;
  }, []);

  if (!admins) return true;
  return admins.some((admin) => admin.toLowerCase() === user.email.toLowerCase());
}
