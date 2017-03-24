BDIR=`pwd`

if [ -d "streaming-data-types" ]
then

if [ ! -d "streaming-data-types/.git" ]
then
echo "THIS LOOKS LIKE A BROKEN BUILD DIRECTORY.  SHOULD WIPE EVERYTHING."
else
cd streaming-data-types && git pull
fi

else
git clone -b master https://github.com/ess-dmsc/streaming-data-types.git
fi


cd $BDIR

if [ -d "googletest" ]
then

if [ ! -d "googletest/.git" ]
then
echo "THIS LOOKS LIKE A BROKEN BUILD DIRECTORY.  SHOULD WIPE EVERYTHING."
else
cd googletest && git pull origin release-1.8.0
fi

else
git clone -b release-1.8.0 https://github.com/google/googletest.git
fi
