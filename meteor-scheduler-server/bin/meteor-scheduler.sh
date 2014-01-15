#!/bin/bash
########################################################################################################################
# 
#  Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
# 
#  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
#  the License. You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
#  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
#  specific language governing permissions and limitations under the License.
# 
########################################################################################################################
#!/bin/bash
########################################################################################################################
# 
#  Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
# 
#  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
#  the License. You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
#  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
#  specific language governing permissions and limitations under the License.
# 
########################################################################################################################

STARTSTOP=$1

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# get nephele config
. "$bin"/nephele-config.sh

if [ "$NEPHELE_PID_DIR" = "" ]; then
        NEPHELE_PID_DIR=/tmp
fi

if [ "$NEPHELE_IDENT_STRING" = "" ]; then
        NEPHELE_IDENT_STRING="$USER"
fi

JVM_ARGS="$JVM_ARGS -Xmx512m"

log=$NEPHELE_LOG_DIR/meteor-scheduler.log
pid=$NEPHELE_PID_DIR/meteor-scheduler-server.pid
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file://"$NEPHELE_CONF_DIR"/log4j.properties"

# auxilliary function to construct a lightweight classpath for the
# PACT CLI client
constructPactCLIClientClassPath() {

	for jarfile in $NEPHELE_LIB_DIR/*.jar ; do
		if [[ $PACT_CC_CLASSPATH = "" ]]; then
			PACT_CC_CLASSPATH=$jarfile;
		else
			PACT_CC_CLASSPATH=$PACT_CC_CLASSPATH:$jarfile
		fi
	done

	for jarfile in $NEPHELE_LIB_DIR/dropin/*.jar ; do
		PACT_CC_CLASSPATH=$PACT_CC_CLASSPATH:$jarfile
	done
	PACT_CC_CLASSPATH=$PACT_CC_CLASSPATH:$NEPHELE_LIB_DIR/dropin/
	
	for jarfile in $NEPHELE_LIB_CLIENTS_DIR/*.jar ; do
		PACT_CC_CLASSPATH=$PACT_CC_CLASSPATH:$jarfile
	done

	echo $PACT_CC_CLASSPATH
}
case $STARTSTOP in

        (start)
                mkdir -p "$NEPHELE_PID_DIR"
                if [ -f $pid ]; then
                        if kill -0 `cat $pid` > /dev/null 2>&1; then
                                echo meteor-scheduler server running as process `cat $pid`.  Stop it first.
                                exit 1
                        fi
                fi
                echo Starting meteor-scheduler server
                PACT_CC_CLASSPATH=`manglePathList $(constructPactCLIClientClassPath)`
		$JAVA_HOME/bin/java $JVM_ARGS $log_setting -classpath $PACT_CC_CLASSPATH eu.stratosphere.meteor.server.DOPAScheduler &
		echo $! > $pid
	;;

        (stop)
                if [ -f $pid ]; then
                        if kill -0 `cat $pid` > /dev/null 2>&1; then
                                echo Stopping meteor-scheduler server
                                kill `cat $pid`
                                kill -9 `cat $pid`
                        else
                                echo No meteor-scheduler server to stop
                        fi
                else
                        echo No meteor-scheduler server to stop
                fi
        ;;

        (*)
                echo Please specify start or stop
        ;;

esac
