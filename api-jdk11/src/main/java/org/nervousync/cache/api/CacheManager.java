/*
 * Copyright 2022 Nervousync Studio
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.nervousync.cache.api;

public interface CacheManager {

	/**
	 * <h3 class="en">Register cache instance by given cache name and config instance</h3>
	 * <h3 class="zhs">使用指定的缓存名称、配置信息注册缓存</h3>
	 *
	 * @param cacheName     <span class="en">Cache name</span>
	 *                      <span class="zhs">缓存名称</span>
	 * @param cacheConfig	<span class="en">Cache config instance</span>
	 *                      <span class="zhs">缓存配置信息</span>
	 */
	boolean register(final String cacheName, final Object cacheConfig);

	/**
	 * <h3 class="en">Retrieve cache client instance by given cache name</h3>
	 * <h3 class="zhs">使用指定的缓存名称获取缓存操作客户端</h3>
	 *
	 * @param cacheName     <span class="en">Cache name</span>
	 *                      <span class="zhs">缓存名称</span>
	 * @return  <span class="en">Cache client instance or null if cache name not registered</span>
	 *          <span class="zhs">缓存客户端实例，若缓存名称未注册则返回null</span>
	 */
	CacheClient client(final String cacheName);

	/**
	 * <h3 class="en">Remove cache instance from registered list</h3>
	 * <h3 class="zhs">移除指定的缓存</h3>
	 *
	 * @param cacheName     <span class="en">Cache name</span>
	 *                      <span class="zhs">缓存名称</span>
	 */
	void deregister(final String cacheName);

	/**
	 * <h3 class="en">Destroy manager instance</h3>
	 * <h3 class="zhs">销毁当前的管理实例</h3>
	 */
	void destroy();
}
