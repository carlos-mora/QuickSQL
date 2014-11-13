/*
 * $Id: mysql.c 11685 2009-07-09 21:22:22Z vszakats $
 */

/*
 * Harbour Project source code:
 * MySQL DBMS low level (client api) interface code.
 *
 * Copyright 2000 Maurilio Longo <maurilio.longo@libero.it>
 * www - http://www.harbour-project.org
 *
 *
 */

#include "hbapi.h"
#include "hbapiitm.h"
#include "hbapifs.h"

#if defined( HB_OS_WIN )
   /* NOTE: To satisfy MySQL headers. */
   #include <winsock2.h>
#endif

#include "mysql.h"

/* NOTE: OS/2 EMX port of MySQL needs libmysqlclient.a from 3.21.33b build which has st and mt
         versions of client library. I'm using ST version since harbour is single threaded.
         You need also .h files from same distribution. */

/* TODO: Use hb_retptrGC() */

#define HB_PARPTR( n ) hb_parptr( n )
#define HB_RETPTR( n ) hb_retptr( n )

const char *key = NULL, *cert = NULL, *ca = NULL, *capath = NULL, *cipher = NULL;

HB_FUNC( MYSQL_GET_SERVER_VERSION ) /* long mysql_get_server_version( MYSQL * ) */
{
#if MYSQL_VERSION_ID > 32399
   hb_retnl( ( long ) mysql_get_server_version( ( MYSQL * ) HB_PARPTR( 1 ) ) );
#else
   const char * szVer = mysql_get_server_info( ( MYSQL * ) HB_PARPTR( 1 ) );
   long lVer = 0;

   while( *szVer )
   {
      if( *szVer >= '0' && *szVer <= '9' )
         lVer = lVer * 10 + *szVer;
      szVer++;
   }
   hb_retnl( lVer );
#endif
}

HB_FUNC( MYSQL_SSL_SET ) /* int mysql_ssl_set(MYSQL *mysql, const char *key, const char *cert, const char *ca, const char *capath, const char *cipher) */
{
   key    = hb_parc( 1 );
   cert   = hb_parc( 2 );
   ca     = hb_parc( 3 );
   capath = hb_parc( 4 );
   cipher = hb_parc( 5 );
}
HB_FUNC( MYSQL_REAL_CONNECT ) /* MYSQL * mysql_real_connect( MYSQL *, char * host, char * user, char * password, char * db, uint port, char *, uint flags ) */
{
   const char * szHost = hb_parc( 1 );
   const char * szUser = hb_parc( 2 );
   const char * szPass = hb_parc( 3 );

#if MYSQL_VERSION_ID > 32200
   unsigned int port  = HB_ISNUM( 4 ) ? ( unsigned int ) hb_parni( 4 ) : MYSQL_PORT;
   unsigned int flags = HB_ISNUM( 5 ) ? ( unsigned int ) hb_parni( 5 ) : 0;
   MYSQL * mysql;

   if( ( mysql = mysql_init( ( MYSQL * ) NULL ) ) != NULL )
   {
      /* from 3.22.x of MySQL there is a new parameter in mysql_real_connect() call, that is char * db
         which is not used here */
      if ( key || cert || ca || capath || cipher )
      {
         mysql_ssl_set( mysql, key, cert, ca, capath, cipher );
      }

      if( mysql_real_connect( mysql, szHost, szUser, szPass, 0, port, NULL, flags ) )
      {
         HB_RETPTR( ( void * ) mysql );
      }
      else
      {
         mysql_close( mysql );
         HB_RETPTR( NULL );
      }
   }
   else
      HB_RETPTR( NULL );
#else
   HB_RETPTR( ( void * ) mysql_real_connect( NULL, szHost, szUser, szPass, 0, NULL, 0 ) );
#endif
}

HB_FUNC( MYSQL_CLOSE ) /* void mysql_close( MYSQL * mysql ) */
{
   mysql_close( ( MYSQL * ) HB_PARPTR( 1 ) );
}

HB_FUNC( MYSQL_COMMIT ) /* bool mysql_commit( MYSQL * mysql ) */
{
#if MYSQL_VERSION_ID >= 40100
   hb_retnl( ( long ) mysql_commit( ( MYSQL * ) HB_PARPTR( 1 ) ) );
#else
   hb_retnl( ( long ) mysql_query( ( MYSQL * ) HB_PARPTR( 1 ), "COMMIT" ) );
#endif
}

HB_FUNC( MYSQL_ROLLBACK ) /* bool mysql_rollback( MYSQL * mysql ) */
{
#if MYSQL_VERSION_ID >= 40100
   hb_retnl( ( long ) mysql_rollback( ( MYSQL * ) HB_PARPTR( 1 ) ) );
#else
   hb_retnl( ( long ) mysql_query( ( MYSQL * ) HB_PARPTR( 1 ), "ROLLBACK" ) );
#endif
}

HB_FUNC( MYSQL_SELECT_DB ) /* int mysql_select_db( MYSQL *, char * ) */
{
   hb_retnl( ( long ) mysql_select_db( ( MYSQL * ) HB_PARPTR( 1 ), ( const char * ) hb_parc( 2 ) ) );
}

HB_FUNC( MYSQL_QUERY ) /* int mysql_query( MYSQL *, char * ) */
{
   hb_retnl( ( long ) mysql_query( ( MYSQL * ) HB_PARPTR( 1 ), hb_parc( 2 ) ) );
}

HB_FUNC( MYSQL_STORE_RESULT ) /* MYSQL_RES * mysql_store_result( MYSQL * ) */
{
   HB_RETPTR( ( void * ) mysql_store_result( ( MYSQL * ) HB_PARPTR( 1 ) ) );
}

HB_FUNC( MYSQL_USE_RESULT ) /* MYSQL_RES * mysql_use_result( MYSQL * ) */
{
   HB_RETPTR( ( void * ) mysql_use_result( ( MYSQL * ) HB_PARPTR( 1 ) ) );
}

HB_FUNC( MYSQL_FREE_RESULT ) /* void mysql_free_result( MYSQL_RES * ) */
{
   mysql_free_result( ( MYSQL_RES * ) HB_PARPTR( 1 ) );
}

HB_FUNC( MYSQL_FETCH_ROW ) /* MYSQL_ROW * mysql_fetch_row( MYSQL_RES * ) */
{
   MYSQL_RES * mresult = ( MYSQL_RES * ) HB_PARPTR( 1 );
   int num_fields = mysql_num_fields( mresult );
   PHB_ITEM aRow = hb_itemArrayNew( num_fields );
   MYSQL_ROW mrow = mysql_fetch_row( mresult );

   if( mrow )
   {
      unsigned long * lengths = mysql_fetch_lengths( mresult );
      int i;
      for( i = 0; i < num_fields; i++ )
         hb_arraySetCL( aRow, i + 1, mrow[ i ], lengths[ i ] );
   }

   hb_itemReturnRelease( aRow );
}

HB_FUNC( MYSQL_DATA_SEEK ) /* void mysql_data_seek( MYSQL_RES *, unsigned int ) */
{
   mysql_data_seek( ( MYSQL_RES * ) HB_PARPTR( 1 ), ( unsigned int ) hb_parni( 2 ) );
}

HB_FUNC( MYSQL_NUM_ROWS ) /* my_ulongulong mysql_num_rows( MYSQL_RES * ) */
{
   hb_retnint( mysql_num_rows( ( ( MYSQL_RES * ) HB_PARPTR( 1 ) ) ) );
}

HB_FUNC( MYSQL_FETCH_FIELD ) /* MYSQL_FIELD * mysql_fetch_field( MYSQL_RES * ) */
{
   /* NOTE: field structure of MySQL has 8 members as of MySQL 3.22.x */
   PHB_ITEM aField = hb_itemArrayNew( 8 );
   MYSQL_FIELD * mfield = mysql_fetch_field( ( MYSQL_RES * ) HB_PARPTR( 1 ) );

   if( mfield )
   {
      hb_arraySetC(  aField, 1, mfield->name );
      hb_arraySetC(  aField, 2, mfield->table );
      hb_arraySetC(  aField, 3, mfield->def );
      hb_arraySetNL( aField, 4, ( long ) mfield->type );
      hb_arraySetNL( aField, 5, mfield->length );
      hb_arraySetNL( aField, 6, mfield->max_length );
      hb_arraySetNL( aField, 7, mfield->flags );
      hb_arraySetNL( aField, 8, mfield->decimals );
   }

   hb_itemReturnRelease( aField );
}

HB_FUNC( MYSQL_FIELD_SEEK ) /* MYSQL_FIELD_OFFSET mysql_field_seek( MYSQL_RES *, MYSQL_FIELD_OFFSET ) */
{
   mysql_field_seek( ( MYSQL_RES * ) HB_PARPTR( 1 ), ( MYSQL_FIELD_OFFSET ) hb_parni( 2 ) );
}

HB_FUNC( MYSQL_NUM_FIELDS ) /* unsigned int mysql_num_fields( MYSQL_RES * ) */
{
   hb_retnl( mysql_num_fields( ( ( MYSQL_RES * ) HB_PARPTR( 1 ) ) ) );
}

#if MYSQL_VERSION_ID > 32200

HB_FUNC( MYSQL_FIELD_COUNT ) /* unsigned int mysql_field_count( MYSQL * ) */
{
   hb_retnl( mysql_field_count( ( ( MYSQL * ) HB_PARPTR( 1 ) ) ) );
}

#endif

HB_FUNC( MYSQL_LIST_FIELDS ) /* MYSQL_RES * mysql_list_fields( MYSQL *, char * ); */
{
   hb_retptr( mysql_list_fields( ( MYSQL * ) HB_PARPTR( 1 ), hb_parc( 2 ), NULL ) );
}

HB_FUNC( MYSQL_ERROR ) /* char * mysql_error( MYSQL * ); */
{
   hb_retc( mysql_error( ( MYSQL * ) HB_PARPTR( 1 ) ) );
}

HB_FUNC( MYSQL_LIST_DBS ) /* MYSQL_RES * mysql_list_dbs( MYSQL *, char * wild ); */
{
   MYSQL * mysql = ( MYSQL * ) HB_PARPTR( 1 );
   MYSQL_RES * mresult = mysql_list_dbs( mysql, NULL );
   long nr = ( long ) mysql_num_rows( mresult );
   PHB_ITEM aDBs = hb_itemArrayNew( nr );
   long i;

   for( i = 0; i < nr; i++ )
   {
      MYSQL_ROW mrow = mysql_fetch_row( mresult );
      hb_arraySetC( aDBs, i + 1, mrow[ 0 ] );
   }

   mysql_free_result( mresult );

   hb_itemReturnRelease( aDBs );
}

HB_FUNC( MYSQL_LIST_TABLES ) /* MYSQL_RES * mysql_list_tables( MYSQL *, char * wild ); */
{
   MYSQL * mysql = ( MYSQL * ) HB_PARPTR( 1 );
   const char * cWild = hb_parc( 2 );
   MYSQL_RES * mresult = mysql_list_tables( mysql, cWild );
   long nr = ( long ) mysql_num_rows( mresult );
   PHB_ITEM aTables = hb_itemArrayNew( nr );
   long i;

   for( i = 0; i < nr; i++ )
   {
      MYSQL_ROW mrow = mysql_fetch_row( mresult );
      hb_arraySetC( aTables, i + 1, mrow[ 0 ] );
   }

   mysql_free_result( mresult );
   hb_itemReturnRelease( aTables );
}

HB_FUNC( MYSQL_AFFECTED_ROWS )
{
   hb_retnl( ( long ) mysql_affected_rows( ( MYSQL * ) HB_PARPTR( 1 ) ) );
}

HB_FUNC( MYSQL_INSERT_ID )
{
   hb_retnl( ( long ) mysql_insert_id( ( MYSQL * ) HB_PARPTR( 1 ) ) );
}


HB_FUNC( MYSQL_GET_HOST_INFO )
{
   hb_retc( mysql_get_host_info( ( MYSQL * ) HB_PARPTR( 1 ) ) );
}

HB_FUNC( MYSQL_GET_SERVER_INFO )
{
   hb_retc( mysql_get_server_info( ( MYSQL * ) HB_PARPTR( 1 ) ) );
}

HB_FUNC( MYSQL_ESCAPE_STRING )
{
   const char * from = hb_parcx( 1 );
   int iSize = hb_parclen( 1 );
   char * buffer = ( char * ) hb_xgrab( iSize * 2 + 1 );
   iSize = mysql_escape_string( buffer, from, iSize );
   hb_retclen_buffer( ( char * ) buffer, iSize );
}

static char * filetoBuff( const char * fname, int * size )
{
   char * buffer = NULL;
   HB_FHANDLE handle = hb_fsOpen( fname, FO_READWRITE );

   if( handle != FS_ERROR )
   {
      *size = ( int ) hb_fsSeek( handle, 0, FS_END );
      hb_fsSeek( handle, 0, FS_SET );
      buffer = ( char * ) hb_xgrab( *size + 1 );
      *size = hb_fsReadLarge( handle, buffer, *size );
      buffer[ *size ] = '\0';
      hb_fsClose( handle );
   }
   else
      *size = 0;

   return buffer;
}

HB_FUNC( MYSQL_ESCAPE_STRING_FROM_FILE )
{
   int iSize;
   char * from = filetoBuff( hb_parc( 1 ), &iSize );

   if( from )
   {
      char *buffer = ( char * ) hb_xgrab( iSize * 2 + 1 );
      iSize = mysql_escape_string( buffer, from, iSize );
      hb_retclen_buffer( buffer, iSize );
      hb_xfree( from );
   }
}
