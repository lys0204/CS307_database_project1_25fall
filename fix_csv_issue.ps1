# 快速修复：从 Git 中移除 CSV 文件
# 使用方法：如果还没有成功 push 到远程，运行此脚本

Write-Host "=== 从 Git 历史中移除 CSV 文件 ===" -ForegroundColor Yellow
Write-Host ""

# 检查是否已经 push 过
$hasRemote = git remote -v 2>&1
if ($hasRemote -match "fetch") {
    Write-Host "检测到远程仓库，将使用安全方法..." -ForegroundColor Yellow
    Write-Host "如果这是你的第一个提交且还没有成功 push，建议使用方案1（重新初始化）" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "按任意键继续查看详细解决方案，或 Ctrl+C 取消..."
    $null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
} else {
    Write-Host "未检测到远程仓库，可以使用重新初始化方法" -ForegroundColor Green
}

Write-Host ""
Write-Host "请查看 SOLUTION_README.md 文件获取详细步骤" -ForegroundColor Green

