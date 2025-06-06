# FairQueue 内部データ構造と依存関係管理

## 概要

このドキュメントでは、FairQueueの内部Redisデータ構造について、特に依存関係管理システムに焦点を当てた詳細な技術仕様を提供します。これらの内部構造を理解することは、FairQueueのコア機能開発や複雑な依存関係シナリオのデバッグにおいて重要です。

## 目次

1. [Redisキー構造](#redisキー構造)
2. [依存関係管理システム](#依存関係管理システム)
3. [キー利用パターン](#キー利用パターン)
4. [双方向依存関係追跡](#双方向依存関係追跡)
5. [状態遷移とクリーンアップ](#状態遷移とクリーンアップ)
6. [使用例とユースケース](#使用例とユースケース)

## Redisキー構造

FairQueueは統一された`fq:`プレフィックスを使用してすべてのRedisキーの一貫性を保ち、競合を回避しています。

### コアキューキー

```
# ユーザーと優先度別のタスクキュー
fq:queue:user:{user_id}:critical    # Priority.CRITICAL タスク (List, FIFO)
fq:queue:user:{user_id}:normal      # Priority 1-5 タスク (Sorted Set, スコアベース)

# 状態管理
fq:state:{state}                    # タスク状態レジストリ (Set)
fq:task:{task_id}                   # タスクメタデータ & 依存関係 (Hash)

# 依存関係追跡（双方向）
fq:deps:waiting:{task_id}           # このタスクを待っているタスク (Set)
fq:deps:blocked:{task_id}           # このタスクをブロックしているタスク (Set)

# タスク間通信とスケジューリング
fq:xcom:{key}                       # XComデータストレージ (Hash with TTL)
fq:stats                           # 統合統計情報 (Hash)
fq:schedules                       # スケジュールされたタスク (Hash)
fq:scheduler:lock                  # スケジューラー分散ロック
```

## 依存関係管理システム

FairQueueは双方向Redisセットを使用してタスク関係を効率的に追跡する洗練された依存関係管理システムを実装しています。

### 核となる概念

- **依存関係**: タスクAが完了するまでタスクBが実行できない関係
- **待機セット**: 特定のタスクの完了を待っているタスクの集合
- **ブロックセット**: 特定のタスクの実行をブロックしているタスクの集合
- **DEFERRED状態**: 依存関係を待っているタスクは準備ができるまでこの状態にとどまる

### 主要コンポーネント

#### 1. `fq:deps:waiting:{task_id}`

**目的**: `{task_id}`の完了を待っているタスクIDを格納

**データ型**: Redis SET

**利用方法**:
- **作成**: タスクBがタスクAに依存する場合、タスクBのIDが`fq:deps:waiting:A`に追加される
- **読み取り**: タスクAが完了したとき、このセットを読み取ってすべての依存タスクを見つける
- **クリーンアップ**: タスクAがキャンセルまたは完了したとき、このセットが処理され削除される

**実装場所**: 
- 作成: `fairque/scripts/push.lua` (115行目)
- 読み取り: `fairque/scripts/state_ops.lua` (86行目)
- クリーンアップ: `fairque/scripts/state_ops.lua` (213-216行目)

#### 2. `fq:deps:blocked:{task_id}`

**目的**: `{task_id}`の実行をブロックしているタスクIDを格納

**データ型**: Redis SET

**利用方法**:
- **作成**: タスクBがタスクAに依存する場合、タスクAのIDが`fq:deps:blocked:B`に追加される
- **読み取り**: タスクに残っている依存関係があるかチェック（`SCARD`）
- **変更**: 完了した依存関係IDを削除
- **クリーンアップ**: タスクが完了またはキャンセルされたときに削除

**実装場所**:
- 作成: `fairque/scripts/push.lua` (116行目)
- 読み取り: `fairque/scripts/state_ops.lua` (94, 277行目)
- 変更: `fairque/scripts/state_ops.lua` (91行目)
- クリーンアップ: `fairque/scripts/state_ops.lua` (121, 223, 247行目)

## キー利用パターン

### 1. 依存関係のあるタスクのエンキュー

```lua
-- push.lua内（114-117行目）
for _, dep_task_id in ipairs(depends_on) do
    redis.call("SADD", DEPS_PREFIX .. "waiting:" .. dep_task_id, task_id)
    redis.call("SADD", DEPS_PREFIX .. "blocked:" .. task_id, dep_task_id)
end
```

**プロセス**:
1. 各依存関係について、新しいタスクを依存先の待機セットに追加
2. 各依存関係を新しいタスクのブロックセットに追加
3. タスク状態を`QUEUED`ではなく`DEFERRED`に設定

### 2. タスク完了時の依存関係解決

```lua
-- state_ops.lua内（86-102行目）
local waiting_tasks = redis.call("SMEMBERS", DEPS_PREFIX .. "waiting:" .. task_id)

for _, waiting_task_id in ipairs(waiting_tasks) do
    -- 待機中タスクからこの依存関係を削除
    redis.call("SREM", DEPS_PREFIX .. "blocked:" .. waiting_task_id, task_id)
    
    -- すべての依存関係が解決されたかチェック
    local remaining_deps = redis.call("SCARD", DEPS_PREFIX .. "blocked:" .. waiting_task_id)
    
    if remaining_deps == 0 then
        -- すべての依存関係が解決 - DEFERRED → QUEUEDに遷移
        -- 実行のためにタスクをユーザーキューに追加
    end
end
```

**プロセス**:
1. 完了したタスクを待っているすべてのタスクを見つける
2. 各待機タスクのブロックセットから完了したタスクを削除
3. 待機タスクがゼロ依存関係になったかチェック
4. 準備ができたタスクを`DEFERRED`から`QUEUED`状態に移動

### 3. タスクキャンセル時のクリーンアップ

```lua
-- state_ops.lua内（213-223行目）
-- 前方参照のクリーンアップ（このタスクを待っているタスク）
local waiting_tasks = redis.call("SMEMBERS", DEPS_PREFIX .. "waiting:" .. task_id)
for _, waiting_task_id in ipairs(waiting_tasks) do
    redis.call("SREM", DEPS_PREFIX .. "blocked:" .. waiting_task_id, task_id)
end
redis.call("DEL", DEPS_PREFIX .. "waiting:" .. task_id)

-- 後方参照のクリーンアップ（このタスクが待っていたタスク）
local blocked_by = redis.call("SMEMBERS", DEPS_PREFIX .. "blocked:" .. task_id)
for _, blocking_task_id in ipairs(blocked_by) do
    redis.call("SREM", DEPS_PREFIX .. "waiting:" .. blocking_task_id, task_id)
end
redis.call("DEL", DEPS_PREFIX .. "blocked:" .. task_id)
```

**プロセス**:
1. 前方参照をクリーンアップ（待機セットからキャンセルされたタスクを削除）
2. 後方参照をクリーンアップ（ブロックセットからキャンセルされたタスクを削除）
3. キャンセルされたタスクの両方の依存関係セットを削除

## 双方向依存関係追跡

システムは効率的な依存関係管理を可能にする補完的なデータ構造を使用しています：

### 関係構造

```
タスクBがタスクAに依存する場合:

fq:deps:waiting:A = {B}    # タスクAにはタスクBが待っている
fq:deps:blocked:B = {A}    # タスクBはタスクAによってブロックされている
```

### 双方向設計の利点

1. **前方解決**: タスクAが完了したとき、依存タスクを素早く見つける
2. **逆引き**: タスクBがキャンセルされたとき、参照を素早くクリーンアップ
3. **依存関係カウント**: `SCARD fq:deps:blocked:B`を使用して残りの依存関係をチェック
4. **アトミック操作**: すべての依存関係操作はLuaスクリプト内でアトミック

### 複雑な依存関係の例

```
シナリオ:
- タスクDはタスクA、B、Cに依存
- タスクEはタスクA、Bに依存  
- タスクFはタスクCに依存

Redis構造:
fq:deps:waiting:A = {D, E}     # AはD、Eに待たれている
fq:deps:waiting:B = {D, E}     # BはD、Eに待たれている
fq:deps:waiting:C = {D, F}     # CはD、Fに待たれている

fq:deps:blocked:D = {A, B, C}  # DはA、B、Cによってブロックされている
fq:deps:blocked:E = {A, B}     # EはA、Bによってブロックされている
fq:deps:blocked:F = {C}        # FはCによってブロックされている
```

### 解決フローの例

1. **タスクA完了時**:
   - `fq:deps:waiting:A = {D, E}`を読み取り
   - `fq:deps:blocked:D`からAを削除 → `{B, C}`
   - `fq:deps:blocked:E`からAを削除 → `{B}`
   - DとEともにまだ依存関係があり、`DEFERRED`のまま

2. **タスクB完了時**:
   - `fq:deps:waiting:B = {D, E}`を読み取り
   - `fq:deps:blocked:D`からBを削除 → `{C}`
   - `fq:deps:blocked:E`からBを削除 → `{}`（空）
   - **タスクEが`QUEUED`状態に遷移**（残りの依存関係なし）

3. **タスクC完了時**:
   - `fq:deps:waiting:C = {D, F}`を読み取り
   - `fq:deps:blocked:D`からCを削除 → `{}`（空）
   - `fq:deps:blocked:F`からCを削除 → `{}`（空）
   - **タスクDとFの両方が`QUEUED`状態に遷移**

## 状態遷移とクリーンアップ

### 依存関係に関連するタスク状態

- **`SCHEDULED`**: `execute_after`時刻を待機中（依存関係に関係なく）
- **`DEFERRED`**: 未解決の依存関係があり、まだキューに入れることができない
- **`QUEUED`**: 実行準備完了（すべての依存関係が解決済み）
- **`STARTED`**: 現在実行中
- **`FINISHED`**: 正常に完了（依存関係解決をトリガー）
- **`FAILED`**: 実行失敗（依存関係は解決されない）
- **`CANCELED`**: 手動でキャンセル（クリーンアップをトリガー）

### 状態遷移ルール

1. **依存関係のあるタスク** → 初期状態は`DEFERRED`
2. **すべての依存関係が解決** → `DEFERRED` → `QUEUED`
3. **タスク完了** → 待機中タスクの依存関係を解決
4. **タスク失敗** → 依存関係は未解決のまま、待機中タスクは`DEFERRED`のまま
5. **タスクキャンセル** → すべての依存関係参照をクリーンアップ

### クリーンアップメカニズム

1. **完了クリーンアップ**: タスク終了時に依存関係セットを削除
2. **キャンセルクリーンアップ**: すべての参照の双方向クリーンアップ
3. **TTLクリーンアップ**: 失敗したタスクデータの自動期限切れ（7日間）
4. **メンテナンスクリーンアップ**: 孤立した依存関係データの定期的なクリーンアップ

## 使用例とユースケース

### 例1: 線形パイプライン

```python
@task(task_id="extract")
def extract_data():
    return {"records": 1000}

@task(task_id="transform", depends_on=["extract"])
def transform_data():
    return {"processed": 2000}

@task(task_id="load", depends_on=["transform"])  
def load_data():
    return {"loaded": True}
```

**Redis構造**:
```
fq:deps:waiting:extract = {transform}
fq:deps:waiting:transform = {load}

fq:deps:blocked:transform = {extract}
fq:deps:blocked:load = {transform}
```

**実行フロー**:
1. すべてのタスクが`DEFERRED`状態で作成される（`extract`を除く）
2. `extract`完了 → `transform`が`QUEUED`に移動
3. `transform`完了 → `load`が`QUEUED`に移動

### 例2: ファンアウト/ファンインパターン

```python
@task(task_id="prepare")
def prepare_data():
    return {"data": "prepared"}

@task(task_id="process_a", depends_on=["prepare"])
def process_a():
    return {"result_a": "done"}

@task(task_id="process_b", depends_on=["prepare"])
def process_b():
    return {"result_b": "done"}

@task(task_id="combine", depends_on=["process_a", "process_b"])
def combine_results():
    return {"combined": "complete"}
```

**Redis構造**:
```
fq:deps:waiting:prepare = {process_a, process_b}
fq:deps:waiting:process_a = {combine}
fq:deps:waiting:process_b = {combine}

fq:deps:blocked:process_a = {prepare}
fq:deps:blocked:process_b = {prepare}
fq:deps:blocked:combine = {process_a, process_b}
```

**実行フロー**:
1. `prepare`完了 → `process_a`と`process_b`の両方が`QUEUED`に移動
2. `process_a`完了 → `combine`はまだ`process_b`に依存
3. `process_b`完了 → `combine`が`QUEUED`に移動

### 例3: 循環検出

FairQueueはタスク作成時に依存関係の循環を防ぎます。循環が検出された場合、タスク作成は検証エラーで失敗し、デッドロック状況を防ぎます。

## パフォーマンス考慮事項

### 最適化戦略

1. **アトミック操作**: すべての依存関係操作でアトミック性のためLuaスクリプトを使用
2. **効率的なセット操作**: Redis SET操作は追加/削除がO(1)
3. **最小限のデータ保存**: 完全なタスクオブジェクトではなく、タスクIDのみを保存
4. **遅延クリーンアップ**: 依存関係データはタスク完了時にクリーンアップ
5. **TTL管理**: 失敗したタスクとその依存関係は自動期限切れ

### 監視ポイント

- **依存関係セットサイズ**: 依存関係セットの`SCARD`を監視
- **DEFERRED タスク数**: `DEFERRED`状態のタスクを追跡
- **依存関係解決時間**: 依存関係完了からタスクキューイングまでの時間
- **孤立した依存関係**: 対応するタスクのない依存関係セット

この内部ドキュメントは、FairQueueの洗練された依存関係管理システムを理解し、作業するための技術的基盤を提供します。