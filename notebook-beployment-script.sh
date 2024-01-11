file_list=$(git diff --name-status HEAD~1 HEAD)
echo "$file_list"