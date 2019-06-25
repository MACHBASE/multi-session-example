#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/time.h>
#include <machbase_sqlcli.h>

int  gSessionCount = 0;
long gRecordPerThr = 0;

void db_connect(SQLHENV * aEnv, SQLHDBC * aCon, int aPort)
{
    char         sConStr[1024];
    SQLINTEGER   sErrorNo;
    SQLSMALLINT  sMsgLength;
    char         sErrorMsg[1024];
    SQLHENV      sEnv;
    SQLHDBC      sCon;

    if (SQL_ERROR == SQLAllocEnv(&sEnv))
    {
        printf("SQLAllocEnv error!!\n");
        exit(1);
    }
    if (SQL_ERROR == SQLAllocConnect(sEnv, &sCon))
    {
        printf("SQLAllocConnect error!!\n");
        SQLFreeEnv(sEnv);
        exit(1);
    }
    snprintf(sConStr,
             sizeof(sConStr),
             "DSN=%s;UID=SYS;PWD=MANAGER;CONNTYPE=1;PORT_NO=%d",
             "127.0.0.1",
             aPort);
    if (SQL_ERROR == SQLDriverConnect(sCon, NULL,
                                      (SQLCHAR *)sConStr,
                                      SQL_NTS,
                                      NULL, 0, NULL,
                                      SQL_DRIVER_NOPROMPT))
    {
        printf("connection error\n");
        if (SQL_SUCCESS == SQLError(sEnv, sCon, NULL, NULL, &sErrorNo,
                                    (SQLCHAR *)sErrorMsg, 1024, &sMsgLength))
        {
            printf(" rCM_-%d : %s\n", sErrorNo, sErrorMsg);
        }
        SQLFreeEnv(sEnv);
        exit(1);
    }
    *aEnv = sEnv;
    *aCon = sCon;
}

void db_disconnect(SQLHENV * aEnv, SQLHDBC * aCon)
{
    SQLINTEGER   sErrorNo;
    SQLSMALLINT  sMsgLength;
    char         sErrorMsg[1024];

    if (SQL_ERROR == SQLDisconnect(*aCon))
    {
        printf("disconnect error\n");
        if (SQL_SUCCESS == SQLError(*aEnv, *aCon, NULL, NULL, &sErrorNo,
                                    (SQLCHAR *)sErrorMsg, 1024, &sMsgLength))
        {
            printf(" rCM_-%d : %s\n", sErrorNo, sErrorMsg);
        }
    }
    SQLFreeConnect(*aCon);
    SQLFreeEnv(*aEnv);
}

void outError(SQLHDBC aCon, SQLHENV aEnv, const char *aMsg, SQLHSTMT aStmt)
{
    SQLINTEGER  sErrorNo;
    SQLSMALLINT sMsgLength;
    char        sErrorMsg[1024];

    printf("ERROR : (%s) \n", aMsg);

    if (SQL_SUCCESS == SQLError(aEnv, aCon, aStmt, NULL, &sErrorNo,
                                (SQLCHAR *)sErrorMsg, 1024, &sMsgLength))
    {
        printf(" mach-%05d : %s\n", sErrorNo, sErrorMsg);
    }

    exit(-1);
}

time_t getTimeStamp()
{
    struct timeval sTimeVal;
    int            sRet;

    sRet = gettimeofday(&sTimeVal, NULL);

    if (sRet == 0)
    {
        return (time_t)(sTimeVal.tv_sec * 1000000 + sTimeVal.tv_usec);
    }
    else
    {
        return 0;
    }
}

void testSelect(SQLHDBC aCon, SQLHENV aEnv, long* aRowCount)
{
    char        * sSQL = "select count(*) from syslog_table";
    SQLHSTMT    sStmt;
    char        ulongData[21] = {0, };
    SQLLEN      sLen = 0;

    if (SQLAllocStmt(aCon, &sStmt) == SQL_ERROR)
    {
        outError(aCon, aEnv, "AllocStmt", sStmt);
    }
    if (SQLPrepare(sStmt, (SQLCHAR*)sSQL, SQL_NTS) == SQL_ERROR)
    {
        printf("Prepare error[%s]\n", sSQL);
        outError(aCon, aEnv, "Prepare error", sStmt);
    }
    if (SQLExecute(sStmt) == SQL_ERROR)
    {
        outError(aCon, aEnv, "Prepared-Execute error", sStmt);
    }
    if (SQLBindCol(sStmt, 1, SQL_C_CHAR, ulongData, sizeof(ulongData), &sLen) != SQL_SUCCESS)
    {
        outError(aCon, aEnv, "bindcol c_char error", sStmt);
    }

    if (SQLFetch(sStmt) == SQL_SUCCESS)
    {
        *aRowCount = atol(ulongData);
    }

    if (SQLFreeStmt(sStmt, SQL_DROP) == SQL_ERROR)
    {
        outError(aCon, aEnv, "FreeStmt", sStmt);
    }

    return;
}

void testAppend(SQLHDBC aCon, SQLHENV aEnv, int aIteratorNum, long * oOutputCount)
{
    SQLHSTMT           sStmt;
    int                i = 0;
    int                rc = 0;
    SQLBIGINT          sSuccessCount = 0;
    SQLBIGINT          sFailureCount = 0;
    SQL_APPEND_PARAM   sParam[10];
    char             * sStr1 = "395 DEBUG kernel:SELinux: initialized (dev tmpfs, type tmpfs)";
    char             * sStr2 = "uses transition SIDs";

    memset(sParam, 0, sizeof(sParam));

    if (SQLAllocStmt(aCon, &sStmt) == SQL_ERROR) {
        outError(aCon, aEnv, "AllocStmt append", sStmt);
    }
    if( SQLAppendOpen(sStmt, (SQLCHAR *)"syslog_table", 100) != SQL_SUCCESS )
    {
        outError(aCon, aEnv, "SQLAppendOpen", sStmt);
    }

    for (i = 0; i < aIteratorNum; i++)
    {
        sParam[0].mDateTime.mTime = SQL_APPEND_DATETIME_STRING;
        sParam[0].mDateTime.mDateStr = "Mar 24 23:28:25";
        sParam[0].mDateTime.mFormatStr = "%b %d %H:%M:%S";
        sParam[1].mShort = 2;
        sParam[2].mInteger = 4;
        sParam[3].mLong = 6;
        sParam[4].mFloat = 8.4;
        sParam[5].mDouble = 10.9;
        sParam[6].mVarchar.mLength = strlen(sStr1);
        sParam[6].mVarchar.mData = sStr1;
        sParam[7].mVarchar.mLength = strlen(sStr2);
        sParam[7].mVarchar.mData = sStr2;
        sParam[8].mIP.mLength = SQL_APPEND_IP_STRING;
        sParam[8].mIP.mAddrString = "203.212.222.111";
        sParam[9].mIP.mLength = SQL_APPEND_IP_IPV6;
        sParam[9].mIP.mAddr[0] = 127;
        sParam[9].mIP.mAddr[1] = 127;
        sParam[9].mIP.mAddr[2] = 127;
        sParam[9].mIP.mAddr[3] = 127;
        sParam[9].mIP.mAddr[4] = 127;
        sParam[9].mIP.mAddr[5] = 127;
        sParam[9].mIP.mAddr[6] = 127;
        sParam[9].mIP.mAddr[7] = 127;
        sParam[9].mIP.mAddr[8] = 127;
        sParam[9].mIP.mAddr[9] = 127;
        sParam[9].mIP.mAddr[10] = 127;
        sParam[9].mIP.mAddr[11] = 127;
        sParam[9].mIP.mAddr[12] = 127;
        sParam[9].mIP.mAddr[13] = 127;
        sParam[9].mIP.mAddr[14] = 127;
        sParam[9].mIP.mAddr[15] = 127;

        if( (rc = SQLAppendDataV2(sStmt, sParam)) != SQL_SUCCESS )
        {
            if (rc == SQL_SUCCESS_WITH_INFO)
            {
                outError(aCon, aEnv, "Append With Info", sStmt);
            }
            else
            {
                outError(aCon, aEnv, "Fail SQLAppendDataV2", sStmt);
            }
        }
        (*oOutputCount)++;
    }

    if( SQLAppendClose(sStmt, (SQLBIGINT*)&sSuccessCount, (SQLBIGINT*)&sFailureCount) != SQL_SUCCESS )
    {
        outError(aCon, aEnv, "SQLAppendClose", sStmt);
    }
    if (SQL_ERROR == SQLFreeStmt(sStmt, SQL_DROP))
    {
        outError(aCon, aEnv, "FreeStmt append", sStmt);
    }
    if (sSuccessCount != aIteratorNum)
    {
        fprintf(stderr, "WARNING!! Append Failure is occured\n");
    }
}

void * testAppendNSelect(void *aPort)
{
    SQLHENV      sEnv;
    SQLHDBC      sCon;
    int          sPort = *((int *) aPort);
    long         sTotalAppendRows = 0;
    int          sIteratorNum = 100;
    long         sRowCount = 0;

    db_connect(&sEnv, &sCon, sPort);

    while (sTotalAppendRows < gRecordPerThr)
    {
        if (sIteratorNum > gRecordPerThr - sTotalAppendRows)
        {
            sIteratorNum = gRecordPerThr - sTotalAppendRows;
        }
        testAppend(sCon, sEnv, sIteratorNum, &sTotalAppendRows);
        testSelect(sCon, sEnv, &sRowCount);
    }

    db_disconnect(&sEnv, &sCon);

    return 0;
}

int main(int argc, char **argv)
{
    int          i = 0;
    int          sResult = 0;
    pthread_t *  sThrArr = NULL;
    int          sPort;
    long         sTotalRecord = 0;
    long         sRowCount = 0;
    time_t       sStartTime, sTimeGap;
    SQLHENV      sEnv;
    SQLHDBC      sCon;

    if (argc >= 3)
    {
        gSessionCount = atol(argv[1]);
        sTotalRecord  = atol(argv[2]);
        gRecordPerThr = sTotalRecord / gSessionCount;
        sPort = atoi(argv[3]);
    }
    else
    {
        fprintf(stderr, "invalid arguments\n");
        exit(-1);
    }

    sThrArr = (pthread_t*)malloc(sizeof(pthread_t) * gSessionCount);
    sStartTime = getTimeStamp();
    for (i = 0; i < gSessionCount; i++)
    {
        sResult = pthread_create(&sThrArr[i], NULL, testAppendNSelect, &sPort);
        if (sResult != 0)
        {
            fprintf(stderr, "pthread create error (index = %d, code = %d)\n", i, sResult);
            exit(-1);
        }
    }

    for (i = 0; i < gSessionCount; i++)
    {
        sResult = pthread_join(sThrArr[i], NULL);
        if (sResult != 0)
        {
            fprintf(stderr, "pthread join error (%d)\n", sResult);
            exit(-1);
        }
    }
    sTimeGap = getTimeStamp() - sStartTime;

    db_connect(&sEnv, &sCon, sPort);
    testSelect(sCon, sEnv, &sRowCount);
    db_disconnect(&sEnv, &sCon);
    printf("Append End\n");
    printf("Concurrent session count : %d\n", gSessionCount);
    printf("timegap = %ld microseconds for %ld records\n", sTimeGap, sRowCount);
    printf("%.2f records/second\n", ((double)sRowCount/(double)sTimeGap)*1000000);

    return 0;
}
