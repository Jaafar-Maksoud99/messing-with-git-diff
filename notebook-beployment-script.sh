file_list=$(git diff --name-status HEAD~1 HEAD | awk 'BEGIN { FS="\t" } (/\.py$/) { print $1, "|" ,$2,"|" ,$3 }')
echo "$file_list"
