#!/bin/bash

rm -rf /Users/igor.simoes/eclipse-workspace/webdispatcherExtension/target/WebDispatcher*log
I=0
J=0
HOST_NUMBER=20
ARRAY_HOSTS=("10.11.1.234" "177.123.23.45" "192.168.1.10" "184.76.180.65" "192.168.2.3" "192.168.2.6" "192.168.2.9" "192.168.2.12" "192.168.2.65" "192.168.2.76" "192.168.2.34" "192.168.2.32" "192.168.2.49" "192.168.2.33" "192.168.2.44" "192.168.2.55" "192.168.2.66" "192.168.2.77" "192.168.2.88" "192.168.2.99")
ARRAY_METHODS=("GET" "POST")
ARRAY_URL=("/sap/bc/srt/wsdl/flv_10002A111AD1/srvc_url/sap/bc/srt/pm/sap/ecc_productionconfidqr/260/local/mes_gsb_cm_profile/1/binding_t_http_a_http_ecc_productionconfidqr_mes_gsb_cm_profile_l?sap-client=260" "/sap/bc/srt/pm/sap/ecc_productionconfidqr/260/local/mes_gsb_cm_profile/1/binding_t_http_a_http_ecc_productionconfidqr_mes_gsb_cm_profile_l" "/" "/sap/bc/srt/wsdl/flv_10002A111AD1/bndg_url/sap/bc/srt/pm/sap/zws_zglpr_create_posting_wm/260/local/mes_gsb_cm_profile/1/binding_t_http_a_http_zws_zglpr_create_posting_wm_mes_gsb_cm_profile_l?sap-client=260" "/sap/bc/srt/pm/sap/zws_zglpr_create_posting_wm/260/local/mes_gsb_cm_profile/1/binding_t_http_a_http_zws_zglpr_create_posting_wm_mes_gsb_cm_profile_l" "/AdapterFramework/util/servlet/DeliveryServlet?target=ejb:localejbs/AF/JobDispatcherBean" "/MessagingSystem/receive/AFW/XI" "/sap/xi/engine?type=entry" "/apple-touch-icon-120x120.png")
ARRAY_CODES=("200" "202" "200" "202" "200" "202" "200" "202" "200" "202" "200" "202" "200" "202" "200" "202" "200" "202" "200" "202" "200" "202" "200" "202" "200" "202" "200" "202" "200" "202" "200" "202" "200" "202" "200" "202" "200" "202" "200" "202" "200" "202" "200" "202" "200" "202" "200" "202" "400" "401" "404" "500" "503")
ARRAY_FQDN=("PIQ.cliente.net" "fioriq.cliente.net" "ECCQ.CLIENTE.NET")
ARRAY_SID=("PI2" "GW2" "GE2")
while [ 0 ]; do 
	RAND_HOST=$(( ( RANDOM % ${#ARRAY_HOSTS[@]} )   ))
	CURR_HOST=${ARRAY_HOSTS[$RAND_HOST]}
	RAND_METHOD=$(( ( RANDOM % ${#ARRAY_METHODS[@]} )   ))
	CURR_METHOD=${ARRAY_METHODS[$RAND_METHOD]}
	RAND_URL=$(( ( RANDOM % ${#ARRAY_URL[@]} )   ))
	CURR_URL=${ARRAY_URL[$RAND_URL]}
	RAND_CODE=$(( ( RANDOM % ${#ARRAY_CODES[@]} )   ))
	CURR_CODE=${ARRAY_CODES[$RAND_CODE]}
	RAND_FQDN=$(( ( RANDOM % ${#ARRAY_FQDN[@]} )   ))
	CURR_FQDN=${ARRAY_FQDN[$RAND_FQDN]}
	CURR_SID=${ARRAY_SID[$RAND_FQDN]}
	TARGET_HOST="gagdb0$(( ( RANDOM % $HOST_NUMBER )   ))"
	#echo "Host: $CURR_HOST, Rand host: $RAND_HOST"
	#echo "Method:  $CURR_METHOD, Rand method: $RAND_METHOD"
	#echo "URL: $CURR_URL, Rand URL: $RAND_URL"
	#echo "Code: $CURR_CODE, Rand code: $RAND_CODE"
	#echo "FQDN: $CURR_FQDN, Rand FQDN: $RAND_FQDN"
	#echo "SID: $CURR_SID, Rand FQDN: $RAND_FQDN"
	SIZE=$(( ( RANDOM % 99999 ) +1 ))
	RT=$(( ( RANDOM % 999 ) +1 ))
	LINE=`date +"[%d/%b/%Y:%T %Z00]"`
	LINE="$LINE $CURR_HOST USER $CURR_METHOD $CURR_URL HTTP/1.1 $CURR_CODE - $SIZE $RT $CURR_FQDN -,-,- - $CURR_SID $TARGET_HOST"
	CURR_DATE=`date +%d%b%Y`
	echo $LINE >> "/Users/igor.simoes/eclipse-workspace/webdispatcherExtension/target/WebDispatcher$CURR_DATE.log"
	echo $LINE
	RAND_SLEEP=$(( ( RANDOM % 9 ) +1 ))
	sleep "0.$RAND_SLEEP"
	I=$(($I + 1))
	if [ $I -eq 100 ]; then
                echo "Rotating..."
		J=$(($J + 1))
                mv "/Users/igor.simoes/eclipse-workspace/webdispatcherExtension/target/WebDispatcher$CURR_DATE.log" "/Users/igor.simoes/eclipse-workspace/webdispatcherExtension/target/WebDispatcher$CURR_DATE-$J.log"
                I=0;
        fi
done  
