// SPDX-License-Identifier: Apache-2.0
//! 类型化扩展位。
//!
//! `AppState` 开源核心不再直接持有 mesh/HA/OIDC 等闭源专属字段；这些状态
//! 经 [`Extensions`] 按类型挂载，由闭源发行版装配。
//! 缺省（空表）= 免费层：查询返回 `None`，行为与历史版本逐字节一致。

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

type Boxed = Arc<dyn Any + Send + Sync>;

/// 按 `TypeId` 索引的类型化容器（`std::any::TypeId` 版 map，仅存 `Arc`）。
#[derive(Default)]
pub struct Extensions {
    map: HashMap<TypeId, Boxed>,
}

impl Extensions {
    pub fn new() -> Self {
        Self::default()
    }

    /// 挂载一个 `Arc<T>`；同类型重复插入覆盖。
    pub fn insert<T: Any + Send + Sync>(&mut self, value: Arc<T>) {
        self.map.insert(TypeId::of::<T>(), value);
    }

    /// 按类型取出引用；未挂载返回 `None`。
    pub fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|v| (**v).downcast_ref::<T>())
    }
}
