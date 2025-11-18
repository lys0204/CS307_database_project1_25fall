# 从 Git 历史中移除 CSV 文件的脚本

$env:FILTER_BRANCH_SQUELCH_WARNING=1

# CSV 文件列表
$csvFiles = @(
    "final_data/recipes.csv",
    "final_data/reviews.csv", 
    "final_data/user.csv",
    "test_data/test_data.csv",
    "test_data/test_data_10000.csv",
    "test_data/test_data_5000.csv",
    "test_data/test_data_50000.csv"
)

# 构建 git rm 命令
$rmCommand = "git rm --cached --ignore-unmatch " + ($csvFiles -join " ")

# 执行 filter-branch
Write-Host "正在从 Git 历史中移除 CSV 文件..."
git filter-branch --force --index-filter $rmCommand --prune-empty --tag-name-filter cat -- --all

Write-Host "清理完成！"
Write-Host "现在运行以下命令来清理和推送："
Write-Host "  git reflog expire --expire=now --all"
Write-Host "  git gc --prune=now --aggressive"
Write-Host "  git push origin --force --all"

