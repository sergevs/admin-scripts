#!/bin/bash
#
# Cluster management:
# Run arbitrary command on the group of hosts in the host by host or paralel mode
#

script=`basename $0`

usage () {
	[ -n "$1" ] && echo "$1" >&2
	cat >&2 <<-EOD
	$script - Mega script to run command on multiple hosts in batch mode
	Usage: $script [options] config command
	Options:
	  -i              Interactive, single process mode, print output of the command.
	  -v              print output of the commands in multithread mode.
	  -q              Quiet, skip any info messages.
	  -H host_list    List of remote hosts to execute command.
	  -u user         Connect to remote hosts as specified user.
	  -o ssh_options  Set additional ssh options.
	  -d              Dry run, show executed commands without execution.
	  -s              Save configuration to output without command execution.
	  -h              Show this help.
	
	Config is a shell script sourced by $script. Following environment
	variables can be set:
	  host_list       List of remote hosts to execute command.
	  user            Connect to remote hosts as specified user.
	  ssh_options     Additional ssh options.
	  silent          Suppress $script output.
	  interactive     Run command sequentially on one host after another.
	
	Command line option have higher priority than variable in config file.
	Use '-' for config if you have no config file.
	EOD
	exit 1
}

[ $# -lt 1 ] && usage;

while getopts "u:iqH:o:dhsvX:" OPTION_ARGS
do
  case $OPTION_ARGS in
    i     ) cmd_interactive=1; [ -z "$cmd_silent" ] && echo '==Running in the single process mode==';;
    u     ) cmd_user=$OPTARG;;
    q     ) cmd_silent="true";;
    v     ) cmd_verbose="true";;
    H     ) cmd_host_list=$OPTARG;;
    o     ) cmd_ssh_options=$OPTARG;;
    d     ) dry_run="echo"; cmd_interactive=1;;
    s     ) dump_config=1;;
    X     ) undocumented=$OPTARG;;
    h     ) usage;;
    *     ) echo "Unimplemented option chosen.";;   # DEFAULT
  esac
done

shift $(($OPTIND - 1))

if [ "$1" != "-" ]; then
	[ -z "$1" ] && usage "==ERROR configuration file is not specified=="

	type $1 >/dev/null 2>&1

	[ "$?" != "0" ] && usage "==ERROR configuration file $1 is not exists or not specified=="

	source $1
fi

[ -n "$cmd_host_list" ]      && host_list=$cmd_host_list
[ -n "$cmd_user" ]           && user=$cmd_user
[ -n "$cmd_ssh_options" ]    && ssh_options=$cmd_ssh_options
[ -n "$cmd_interactive" ]    && interactive=$cmd_interactive
[ -n "$cmd_silent" ]         && silent=$cmd_silent
[ -n "$cmd_verbose" ]        && verbose=$cmd_verbose

shift

[ -z "$host_list" ] && usage "==ERROR no remote host(s) specified=="

if [ $dump_config ]; then
	[ -n "$host_list" ]      && echo "host_list=$host_list"
	[ -n "$user" ]           && echo "user=$user"
	[ -n "$ssh_options" ]    && echo "ssh_options=$ssh_options"
	[ -n "$interactive" ]    && echo "interactive=$interactive"
	[ -n "$silent" ]         && echo "silent=$silent"
	[ -n "$verbose" ]        && echo "verbose=$verbose"
	exit 0;
fi

[ -n "$user" ] && (echo "$user" | grep -v '@$' >/dev/null 2>&1) && user="$user@"

[ $# -lt 1 ] && usage "==ERROR no command specified=="

[ -z "$host_list" ] && usage "==ERROR \$host_list variable is not defined in <$1>=="

case ${undocumented:-undef} in
	undef) ;;
	stdin) 
			[ -z "$verbose" ] && exit 42
			#interactive is does not allowed with stdin because stdin does not allow terminal
			[ -n "$interactive" ] && exit 42 
			use_stdin=`mktemp ~/tmp/runstdin.XXXXX`
			echo "==WELCOME HACKER. YOU ARE USING STDIN FEATURE==" >&2
			echo "==FLASHING STDIN to $use_stdin FILE=="
			cat /dev/stdin > "$use_stdin"
			echo "==FLASHED=="
			;;
	*) exit 42 ;;
esac

set -m
set -b
[ -z "$silent" ] && echo '==Entering into Exec section=='
icount=0
for i in $host_list 
do
  host_state[$icount]='OK'
	[ -z "$silent" ] && echo -n "${interactive:+== }$i "
#	nc -z $i 22 > /dev/null
	if [ $? -eq 0 ]; then
		if [ -n "$interactive" ]; then
      echo 'out =='
			$dry_run ssh -t $ssh_options $user$i "$pref_command $@"  
		else
			p_out[$icount]=`mktemp ~/tmp/$i.XXXXX`
			if [ -n "$verbose" ]; then
				if [ -z "$use_stdin" ]; then
					ssh -n -T $ssh_options $user$i "$pref_command $@" > "${p_out[$icount]}" 2>&1 & 
				else
					cat "$use_stdin" | ssh -T $ssh_options $user$i "$pref_command $@" > "${p_out[$icount]}" 2>&1 & 
				fi
			else
				ssh -n -T $ssh_options $user$i "$pref_command $@>/dev/null" 2> "${p_out[$icount]}" & 
			fi
		fi
		p_id[$icount]=$!
	else
		down_hosts="$down_hosts $i"
    host_state[$icount]='DOWN'
    [ -n "$interactive" ] && echo
	fi
	icount=$((icount+1))
done  

[ -z "$silent" ] && echo -e "\n==Exec section finished=="
[ -n "$down_hosts" -a -z "$silent" -a -n "$interactive" ] && echo -e "==WARNING: $down_hosts down=="
[ -n "$interactive" ] && exit

set +m
icount=0
for i in $host_list 
do
	[ -z "$silent" ] && echo -n "${i}-" 
	[ -n "${host_state[$icount]##DOWN}" ] && wait ${p_id[$icount]};
	err_code=$?
	[ $err_code -eq 0 ]	&& err_code=''
	if [ -z "$silent" ]; then
    if [ -n "${host_state[$icount]##DOWN}" ]; then
      echo -n "${err_code:-${host_state[$icount]}} "
    else
      echo -n "${host_state[$icount]} "
    fi
  fi
	icount=$((icount+1))
done

[ -f "$use_stdin" ] && rm "$use_stdin"

icount=0
x_out=`mktemp ~/tmp/runX.XXXXX`
for i in $host_list 
do
	if [ -s "${p_out[$icount]}" ]; then
		[ -z "$silent" ] && echo "== $i out==" >> "$x_out"
		cat "${p_out[$icount]}" >> "$x_out"
		[ -z "$silent" ] && echo "==eof $i out==" >> "$x_out"
	fi
	rm -f "${p_out[$icount]}" 
	icount=$((icount+1))
done

[ -z "$silent" ] && echo 
[ -n "$down_hosts" -a -z "$silent" ] && echo -e "==WARNING: $down_hosts down=="

[ -s $x_out ] && less -X $x_out
rm -f $x_out
