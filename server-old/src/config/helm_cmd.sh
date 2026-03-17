#!/bin/sh
helm3 upgrade --install proton-mgnt-tiduyun proton-mgnt-tiduyun-1.0.0-1.tgz \
--set image.registry=acr.aishu.cn \
--set image.dataModelPre.repository=ict/deploymentstudio-dm-pre \
--set image.dataModelPre.tag=git.0b838a53.1325668 \
--set depServices.rds.admin_key='cm9vdDplaXNvby5jb20=' \
--set depServices.rds.user=root --set depServices.rds.type=mysql \
--set depServices.rds.host='10.4.110.84' \
--set depServices.rds.port=3307 \
--set depServices.rds.password=eisoo.com  \
--set depServices.rds.source_type=external  \
--set depServices.rds.type=mysql  \
-n anyshare
