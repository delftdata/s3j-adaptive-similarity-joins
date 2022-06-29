#!/bin/sh

# PATH TO YOUR HOSTS FILE
ETC_HOSTS=/etc/hosts

# DEFAULT IP FOR HOSTNAME
IP=$2

# Hostname to add/remove.
HOSTNAME=$1

if [ -n "$(grep $HOSTNAME /etc/hosts)" ]
then
    echo "$HOSTNAME found in your $ETC_HOSTS, Removing now...";
    echo "4655TuD" | sudo -S sed -i".bak" "/$HOSTNAME/d" $ETC_HOSTS
else
    echo "$HOSTNAME was not found in your $ETC_HOSTS";
fi

HOSTS_LINE="$IP\t$HOSTNAME"

echo "Adding $HOSTNAME to your $ETC_HOSTS";
echo "4655TuD" | sudo -S -- sh -c -e "echo '$HOSTS_LINE' >> /etc/hosts";

if [ -n "$(grep $HOSTNAME /etc/hosts)" ]
    then
        echo "$HOSTNAME was added succesfully \n $(grep $HOSTNAME /etc/hosts)";
    else
        echo "Failed to Add $HOSTNAME, Try again!";
fi
