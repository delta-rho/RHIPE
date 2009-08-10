# TG=`echo "VER=0.45" |python -c 'import sys; print(sys.stdin.read().split("\n"))[0].split("=")[1];'`
# git tag -a -m "v$TG" 
# ##Update git
# git commit -m ''
# git push --tags origin master


# ##To remove a folder
# ## git rm -r dist
# ## git commit -m ''


#git tag -d v0.45
#git push origin :v0.45  