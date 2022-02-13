#!/bin/bash

GITHUB_ACCOUNT=buffalo-cse562-sp22

if [ $# -lt 1 ]; then
    echo "Script for setting up your git repository."
    echo "usage: $0 <your-git-repo-address>"
    echo "You must first create a private repository on Github and give "
    echo "**READ** access to the Github user ${GITHUB_ACCOUNT}."
    echo "<your-git-repo-address> can be found when you click on the green "
    echo "\"code\" button. Make sure you use the SSH link in the form: "
    echo "git@github.com:<your-username>/<your-repository-name>.git"
    exit 1
fi

# The script for setting up the repository for the whole project.
GITADDR=$1

BASEDIR="$(cd "`dirname "$0"`" && pwd)"
cd "$BASEDIR"

if [ -d .git ]; then
    echo "Error: git repo is initialized"
    exit 1
fi

echo \
".vscode
/build
/build.release
/external/install
.DS_Store
/*.tar.xz" > .gitignore

git init || exit 1
git remote add origin $GITADDR || exit 1
mkdir external || exit 1
git submodule add -b master -- https://github.com/jemalloc/jemalloc external/jemalloc || exit 1
git submodule add -b master -- https://github.com/abseil/abseil-cpp.git external/abseil-cpp || exit 1
git submodule add -b main -- https://github.com/google/googletest external/googletest || exit 1
git submodule update --init --recursive || exit 1
git add -A || exit 1
git commit -am "Initial repository setup" || {
	echo ""
	echo "If you see an error above saying \"Please tell me who you are\", "
	echo "  Please follow the instruction to set the email (of your Github account)"
	echo "  and your name first. Then run"
	echo "      git commit -am "Initial repository setup""
	echo "      git branch -m main"
	exit 1
}
# Github now uses ``main'' as the default branch name.
git branch -m main || exit 1


echo ""
echo "Repo setup is finished. Here are a few post-setup steps to follow:"
echo "1. Push the main branch to remote"
echo "      git push -u origin main"
echo "2. Run"
echo "      git status"
echo "   and make sure it outputs something like the following:"
echo "      On branch main"
echo "      Your branch is up to date with 'origin/main'."
echo "      nothing to commit, working tree clean"
echo ""
echo "If you've completed all the above steps, you've successfully set up your"
echo "repository."


