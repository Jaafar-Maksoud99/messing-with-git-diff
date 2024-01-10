file_list=$(git diff --name-status HEAD~1 HEAD | awk 'BEGIN { FS="^[A-Z]+[ \t]+" } !(/^D/ || /^R/) && (/\.py$/) { print "\"" $2 "\"" }')
echo "$file_list"