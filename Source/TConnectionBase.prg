/*
 * Proyecto: QuickSQL
 * Fichero: TConnectionBase.prg
 * Descripción:
 * Autor: Carlos Mora
 * Fecha: 13/10/2011
 */

#include "hbclass.ch"
#include "mysql.ch"
#include "common.ch"

CLASS TConnectionBase
   VAR   nHandle
   VAR   cServer
   VAR   nPort
   VAR   cUser
   VAR   cPassword
   VAR   cDataBase
   VAR   nStatus
   VAR   lError  INIT .F.
   VAR   bLog

   VAR aObjects

   CONSTRUCTOR New()
   METHOD SelectDB()

   METHOD Commit()
   METHOD RollBack()
   METHOD Version()
   METHOD Execute()

   METHOD Query()
   METHOD ScalarQuery()
   MESSAGE Command() METHOD Execute()

   METHOD InsertId()
   METHOD AffectedRows()

   METHOD ErrorMessage()

   DESTRUCTOR End()

   METHOD Register()
   METHOD DeRegister()


ENDCLASS

//------------------------------------------------------------------------------
METHOD New( cServer, cUser, cPassword, nPort, cDataBase )
//------------------------------------------------------------------------------

   IF ValType( nPort ) != 'N'
      nPort:= 3306
   ENDIF

   ::cServer := cServer
   ::nPort := nPort
   ::cUser := cUser
   ::cPassword := cPassword

   ::nHandle := mysql_real_connect( cServer, cUser, cPassword, nPort )

   IF ( ::lError:= Empty( ::nHandle ) )
      ::nHandle:= NIL
   ELSEIF !Empty( cDataBase )
      ::SelectDB( cDataBase )
   ENDIF

RETURN Self

//------------------------------------------------------------------------------
METHOD SelectDB( cDatabase )
//------------------------------------------------------------------------------

   IF mysql_select_db( ::nHandle, cDatabase ) == 0
      ::cDatabase := cDatabase
      ::lError := .F.
      RETURN .T.
   ENDIF

   ::cDatabase := ""
   ::lError := .T.

RETURN .F.


//------------------------------------------------------------------------------
METHOD End()
//------------------------------------------------------------------------------
   LOCAL a

   IF ValType( ::aObjects ) == 'A'
      a:= AClone( ::aObjects )
      ::aObjects:= NIL
      AEval( a, {|x| x:End() } )
   ENDIF
   IF !Empty( ::nHandle )
      mysql_close( ::nHandle )
   ENDIF
   ::nHandle:= NIL
RETURN NIL

//------------------------------------------------------------------------------
METHOD Register( o )
//------------------------------------------------------------------------------
   IF ValType( ::aObjects ) != 'A'
      ::aObjects:= {}
   ENDIF
   AAdd( ::aObjects, o )
RETURN Self

//------------------------------------------------------------------------------
METHOD DeRegister( o )
//------------------------------------------------------------------------------
   LOCAL i
   IF (i := AScan( ::aObjects, o ) ) > 0
      ADel( ::aObjects, i )
      ASize( ::aObjects, Len( ::aObjects ) - 1 )
   ENDIF
RETURN Self

//------------------------------------------------------------------------------
METHOD Commit()
//------------------------------------------------------------------------------
   RETURN mysql_commit( ::nHandle ) == 0

//------------------------------------------------------------------------------
METHOD InsertId()
//------------------------------------------------------------------------------
   RETURN mysql_insert_id( ::nHandle )

//------------------------------------------------------------------------------
METHOD AffectedRows()
//------------------------------------------------------------------------------
   RETURN mysql_affected_rows( ::nHandle )

//------------------------------------------------------------------------------
METHOD RollBack()
//------------------------------------------------------------------------------
   RETURN mysql_rollback( ::nHandle ) == 0

//------------------------------------------------------------------------------
METHOD Version()
//------------------------------------------------------------------------------
   RETURN mysql_get_server_version( ::nHandle )

//------------------------------------------------------------------------------
METHOD Execute( cQuery, ... )
//------------------------------------------------------------------------------
   LOCAL i

   i:= PCount()
   WHILE i > 1
      cQuery:= StrTran( cQuery, '$'+(LTrim(Str(i-1))), ClipValue2SQL( HB_PValue(i) ) )
      i--
   ENDDO
   IF ValType( ::bLog ) == 'B'
      Eval( ::bLog, cQuery )
   ENDIF
RETURN ! ( ::lError := mysql_query( ::nHandle, cQuery ) != 0 )

//------------------------------------------------------------------------------
METHOD Query( cQuery, ... ) // return TResult
//------------------------------------------------------------------------------
   LOCAL oResult, i
   i:= PCount()
   // Replacing query params from last to first. Avoids repeating HB_Param calls in iteration,
   // and the problem of having more than 10 params givin $10 get replaced by $1
   WHILE i > 1
      cQuery:= StrTran( cQuery, '$'+(LTrim(Str(i-1))), ClipValue2SQL( HB_PValue(i) ) )
      i--
   ENDDO

   oResult:= NIL
   IF ::Execute( cQuery )
       oResult:= TResult():New( Self, mysql_store_result( ::nHandle ), cQuery )
   ENDIF

RETURN oResult

//------------------------------------------------------------------------------
METHOD ScalarQuery( cQuery, uDefault, ... ) // returns Value
//------------------------------------------------------------------------------
   LOCAL nResultHandle, nType, uResult, i
   i:= PCount()
   // Replacing query params from last to first. Avoids repeating HB_Param calls in iteration,
   // and the problem of having more than 10 params givin $10 get replaced by $1
   WHILE i > 2
      cQuery:= StrTran( cQuery, '$'+(LTrim(Str(i-2))), ClipValue2SQL( HB_PValue(i) ) )
      i--
   ENDDO

   IF ::Execute( cQuery )

      IF !Empty( nResultHandle := mysql_store_result( ::nHandle ) ) .and. mysql_num_rows( nResultHandle ) > 0

         nType := mysql_fetch_field( nResultHandle )[MYSQL_FS_TYPE]
         uResult := SqlValue2Clip( mysql_fetch_row( nResultHandle )[1], nType )

         mysql_free_result( nResultHandle )

      ENDIF

   ENDIF

   IF uResult == NIL
      uResult := uDefault
   ENDIF

RETURN uResult

//------------------------------------------------------------------------------
METHOD ErrorMessage()
//------------------------------------------------------------------------------
RETURN mysql_error( ::nHandle )

//------------------------------------------------------------------------------
FUNCTION SQLValue2Clip( uValue, origType )
//------------------------------------------------------------------------------

   // ::aFieldStruct[i][MYSQL_FS_TYPE]
   SWITCH origType
   CASE MYSQL_TINY_TYPE
      DEFAULT uValue TO "0"
      uValue := Val( uValue ) != 0
      EXIT

   CASE MYSQL_SHORT_TYPE
   CASE MYSQL_LONG_TYPE
   CASE MYSQL_LONGLONG_TYPE
   CASE MYSQL_INT24_TYPE
   CASE MYSQL_DECIMAL_TYPE
   CASE MYSQL_DOUBLE_TYPE
   CASE MYSQL_FLOAT_TYPE
      DEFAULT uValue TO "0"
      uValue := Val( uValue )
      EXIT

   CASE MYSQL_DATE_TYPE
      IF Empty( uValue )
         uValue := hb_SToD( "" )
      ELSE
         uValue := hb_SToD( Left( uValue, 4 ) + SubStr( uValue, 6, 2 ) + Right( uValue, 2 ) )
      ENDIF
      EXIT

   CASE MYSQL_BLOB_TYPE
      // Memo field
      EXIT

   CASE MYSQL_STRING_TYPE
   CASE MYSQL_VAR_STRING_TYPE
      // char field
      EXIT

   CASE MYSQL_DATETIME_TYPE
      // DateTime field
      EXIT

   OTHERWISE
      // error

   ENDSWITCH

   RETURN uValue
