# 从 Git 历史中移除大 CSV 文件的解决方案

## 问题
CSV 文件（特别是 recipes.csv 598MB 和 reviews.csv 514MB）已经被提交到 Git 历史中，导致无法推送到远程仓库。

## 解决方案

### 方案 1：如果还没有推送到远程（推荐）

如果这是你的第一个提交且还没有 push，最简单的方法是重新初始化仓库：

```powershell
# 1. 备份当前更改
git stash

# 2. 删除 .git 目录（这会删除所有 Git 历史）
Remove-Item -Recurse -Force .git

# 3. 重新初始化 Git 仓库
git init

# 4. 添加 .gitignore（已经更新，包含 CSV 文件）
git add .gitignore

# 5. 添加所有文件（CSV 文件会被 .gitignore 忽略）
git add .

# 6. 创建新的初始提交
git commit -m "Initial commit (without CSV files)"

# 7. 添加远程仓库
git remote add origin https://github.com/lys0204/DataBase-Project1-25Fall.git

# 8. 强制推送（因为历史已改变）
git push -u origin master --force
```

### 方案 2：如果已经推送到远程

如果已经 push 过，需要使用 BFG Repo-Cleaner（推荐）或 git filter-repo：

#### 使用 BFG Repo-Cleaner（最简单）

1. 下载 BFG：https://rtyley.github.io/bfg-repo-cleaner/
2. 运行以下命令：

```powershell
# 克隆一个镜像仓库
git clone --mirror https://github.com/lys0204/DataBase-Project1-25Fall.git

# 使用 BFG 删除 CSV 文件
java -jar bfg.jar --delete-files "*.csv" DataBase-Project1-25Fall.git

# 清理并推送
cd DataBase-Project1-25Fall.git
git reflog expire --expire=now --all
git gc --prune=now --aggressive
git push --force
```

#### 使用 git filter-repo

```powershell
# 安装 git filter-repo（需要 Python）
pip install git-filter-repo

# 删除 CSV 文件
git filter-repo --path final_data/recipes.csv --invert-paths
git filter-repo --path final_data/reviews.csv --invert-paths
git filter-repo --path final_data/user.csv --invert-paths
git filter-repo --path-glob 'test_data/*.csv' --invert-paths

# 强制推送
git push origin --force --all
```

### 方案 3：手动使用 git filter-branch（如果上述方法不可用）

创建一个批处理文件 `remove_csv.bat`：

```batch
@echo off
set FILTER_BRANCH_SQUELCH_WARNING=1
git filter-branch --force --index-filter "git rm --cached --ignore-unmatch final_data/recipes.csv final_data/reviews.csv final_data/user.csv test_data/test_data.csv test_data/test_data_10000.csv test_data/test_data_5000.csv test_data/test_data_50000.csv" --prune-empty --tag-name-filter cat -- --all
git reflog expire --expire=now --all
git gc --prune=now --aggressive
```

然后在 Git Bash 中运行（不是 PowerShell）：
```bash
bash remove_csv.bat
```

## 重要提示

1. **备份**：在执行任何操作前，确保备份你的代码
2. **.gitignore 已更新**：CSV 文件已经被添加到 .gitignore，未来的提交不会包含它们
3. **强制推送**：修改历史后需要使用 `--force` 推送
4. **团队协作**：如果其他人也在使用这个仓库，需要通知他们重新克隆

## 验证

清理后，运行以下命令验证 CSV 文件已从历史中移除：

```powershell
git rev-list --objects --all | Select-String "\.csv"
```

如果没有输出，说明 CSV 文件已成功移除。

