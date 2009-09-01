export R_HOME=/Library/Frameworks/R.framework/Resources
echo trial which unisize real user sys
for trials in 1 100 1000 10000 100000 1000000 10000000; do
    for which in 0 1 2; do
	for unsize in 125 500 2000; do
	    ww="list(x=1,y=2,z=runif($unsize))"
	    z=`(time  trial=$trials which=$which rexpress=$ww ./imperious --silent 1>/dev/null ) 2>&1 | awk '{print $2}'`
	    echo $trials $which $unsize $z
	done
    done
done