# ---- 開始腳本 ----

# 1) 移動到目標目錄
Push-Location "C:\Users\TW0002\Desktop\Wenbin"

# 2) 取得 Git 狀態 (porcelain 模式方便解析)
$diffInfo = git status --porcelain

# 3) 判斷是否有變更
if (-not [string]::IsNullOrWhiteSpace($diffInfo)) {
    Write-Host "✅ 檢測到變更，即將執行備份與提交程序..."

    # 4) 建立備份分支
    $timeStamp  = Get-Date -Format "yyyyMMdd-HHmm"
    $backupName = "backup-$timeStamp"
    git branch $backupName
    Write-Host "📦 已建立備份分支: $backupName"

    # 5) 解析變更檔案清單
    #    Porcelain 輸出每行類似 "?? newfile.txt"、" M changedfile.cs" 等等
    #    下方先用換行分割，再去掉前面狀態碼，只留下檔案路徑
    $changedFiles = $diffInfo -split "[\r\n]+" | ForEach-Object {
        $_ -replace "^\s*\S+\s+", ""  # 去除行首的 status 碼
    } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }

    # 6) 檢查檔案副檔名是否被改變
    foreach ($filePath in $changedFiles) {
        if (Test-Path $filePath) {
            $nowExt = [System.IO.Path]::GetExtension($filePath).TrimStart(".")

            # 透過 git ls-tree HEAD -- <file> 來查當前版本庫對應的檔案路徑
            $lsResult = git ls-tree HEAD -- $filePath 2>$null

            # 如果結果不空，則嘗試從中取出原先副檔名
            if (-not [string]::IsNullOrWhiteSpace($lsResult)) {
                # 典型輸出格式類似：
                # 100644 blob e69de29bb2d1d6434b8b29ae775ad8c2e48c5391    src/main.py
                # 這裡我們以空白分割，再從倒數第一或第二段取檔案路徑
                $parts   = $lsResult -split "\s+"
                $oldPath = $parts[$parts.Count - 1]  # 最後一段應該是完整檔案路徑
                $oldExt  = [System.IO.Path]::GetExtension($oldPath).TrimStart(".")

                if ($oldExt -and ($oldExt -ne $nowExt)) {
                    Write-Host "❌ 發現檔案副檔名改變: $filePath ($oldExt → $nowExt)"
                    Write-Host "⚠️  中止提交程序！"
                    Pop-Location
                    return
                }
            }
        }
    }

    # 7) 若未發現副檔名衝突，則正常自動提交並推送
    git add *
    $commitMsg = "Auto commit: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
    git commit -m $commitMsg
    git push origin main

    Write-Host "🎉 已自動提交並推送變更至 main 分支"
}
else {
    Write-Host "⚡ 沒有偵測到任何變更，跳過提交"
}

# 8) 切回原始工作目錄 (平衡 Push-Location)
Pop-Location

# ---- 結束腳本 ----
