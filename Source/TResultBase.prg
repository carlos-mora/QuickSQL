#include "hbclass.ch"
#include "mysql.ch"
#include "common.ch"

//------------------------------------------------------------------------------
CLASS TResultBase
//------------------------------------------------------------------------------
   VAR   oConnection
   VAR   cQuery
   VAR   hResult      INIT NIL
   VAR   aMetaData    INIT NIL
   VAR   nCursor      INIT -1
   VAR   nCount       INIT -1
   VAR   aRow         INIT NIL

   CONSTRUCTOR New()
   METHOD End()
   DESTRUCTOR Destroy()

   METHOD SetResult( hResult )
   METHOD FreeResult()

   METHOD FieldCount()
   METHOD FieldGet()
   METHOD FieldPos()
   METHOD FieldName()

   METHOD GoTo()
   METHOD GoTop()
   METHOD GoBottom()
   METHOD LastRec()
   MESSAGE RecCount METHOD LastRec()
   METHOD RecNo()
   METHOD Skip()
   METHOD Eof()
   METHOD Bof()

   METHOD ToArray()

   ERROR HANDLER OnError( uParam1 )

ENDCLASS

//------------------------------------------------------------------------------
METHOD New( oConnection, hResult, cQuery )
//------------------------------------------------------------------------------

   ::oConnection := oConnection

   DEFAULT hResult TO mysql_store_result( oConnection:nHandle )

   ::SetResult( hResult, cQuery )

   RETURN Self

//------------------------------------------------------------------------------
METHOD End()
//------------------------------------------------------------------------------
   ::FreeResult()
   IF ::oConnection != NIL
      ::oConnection:DeRegister( Self )
      ::oConnection:= NIL
   ENDIF
RETURN NIL

//------------------------------------------------------------------------------
METHOD Destroy()
//------------------------------------------------------------------------------
RETURN ::End()

//------------------------------------------------------------------------------
METHOD SetResult( hResult, cQuery )
//------------------------------------------------------------------------------
   LOCAL nFieldCount

   ::FreeResult()
   ::cQuery := cQuery

   ::nCount := mysql_num_rows( ::hResult:= hResult )
   ::nCursor:= 0
   nFieldCount := mysql_num_fields( hResult )
   aEval( ::aMetaData := Array( nFieldCount ), {|x,i| ::aMetaData[i]:= mysql_fetch_field( hResult ) } )
   ::aRow := NIL

RETURN Self

//------------------------------------------------------------------------------
METHOD FreeResult()
//------------------------------------------------------------------------------
   IF !empty( ::hResult )
      mysql_free_result( ::hResult )
      ::hResult:= NIL
      ::aMetaData := NIL
      ::aRow := NIL
      ::nCount:= ::nCursor := -1
   ENDIF
RETURN Self

//------------------------------------------------------------------------------
METHOD GoTop()
//------------------------------------------------------------------------------
   ::nCursor:= 1
   ::aRow := NIL
RETURN Self

//------------------------------------------------------------------------------
METHOD GoTo( nRecord )
//------------------------------------------------------------------------------
   ::nCursor:= Min( nRecord, ::nCount )
   ::aRow := NIL
RETURN Self

//------------------------------------------------------------------------------
METHOD GoBottom()
//------------------------------------------------------------------------------
   ::nCursor:= ::nCount
   ::aRow := NIL
RETURN Self

//------------------------------------------------------------------------------
METHOD LastRec()
//------------------------------------------------------------------------------
RETURN ::nCount

//------------------------------------------------------------------------------
METHOD Skip( n )
//------------------------------------------------------------------------------
   LOCAL nSkipped
   IF ValType( n ) != 'N'
      n:= 1
   ENDIF
   IF n > 0
      // DEBUG ::nCount, ::nCursor
      nSkipped:= Min( ::nCount - ::nCursor + 1, n )
   ELSE
      nSkipped:= Max( - ::nCursor, n  )
   ENDIF
   IF nSkipped == 0
   ELSE
      ::aRow := NIL
      ::nCursor+= nSkipped
   ENDIF

RETURN nSkipped

//------------------------------------------------------------------------------
METHOD Eof()
//------------------------------------------------------------------------------
RETURN (::nCount == 0) .OR. ::nCursor > ::nCount

//------------------------------------------------------------------------------
METHOD Bof()
//------------------------------------------------------------------------------
RETURN (::nCount == 0) .OR. ::nCursor < 1

//------------------------------------------------------------------------------
METHOD RecNo()
//------------------------------------------------------------------------------
RETURN ::nCursor

//------------------------------------------------------------------------------
METHOD FieldCount()
//------------------------------------------------------------------------------
RETURN Len( ::aMetaData )

//------------------------------------------------------------------------------
METHOD FieldGet(i)
//------------------------------------------------------------------------------
   IF ::aRow == NIL
      IF ::nCount == 0 .OR. ::nCursor > ::nCount // EOF()
         AFill( ::aRow:= Array( Len( ::aMetaData ) ), '' )
      ELSE
         IF ::nCursor < 0
            ::nCursor:= 1
         ENDIF
         mysql_data_seek( ::hResult, ::nCursor - 1 )
         ::aRow := mysql_fetch_row( ::hResult )
      ENDIF
   ENDIF
RETURN SqlValue2Clip( ::aRow[i], ::aMetaData[i][MYSQL_FS_TYPE] )

//------------------------------------------------------------------------------
METHOD FieldPos(cField)
//------------------------------------------------------------------------------
   cField:= Upper(cField)
RETURN AScan( ::aMetaData, {|x| Upper(x[MYSQL_FS_NAME]) == cField } )

//------------------------------------------------------------------------------
METHOD FieldName(nPos)
//------------------------------------------------------------------------------

RETURN ::aMetaData[nPos][MYSQL_FS_NAME]

//------------------------------------------------------------------------------
METHOD ToArray()
//------------------------------------------------------------------------------
   LOCAL i, j
   LOCAL aResult

   aResult:= Array( ::nCount )

   FOR i:= 1 TO ::nCount
      mysql_data_seek( ::hResult, i - 1 )
      aResult[i] := mysql_fetch_row( ::hResult )
      FOR j:= 1 TO Len( aResult[i] )
         aResult[i][j]:= SqlValue2Clip( aResult[i][j], ::aMetaData[j][MYSQL_FS_TYPE] )
      NEXT
   NEXT
RETURN aResult

//------------------------------------------------------------------------------
METHOD OnError( uParam1 )
//------------------------------------------------------------------------------
   LOCAL cMsg   := __GetMessage()
   LOCAL i

   IF ( i:= AScan( ::aMetaData, {|x| Upper(x[MYSQL_FS_NAME]) == cMsg } ) ) > 0
      RETURN ::FieldGet(i)
   ENDIF

   /*
   IF SubStr( cMsg, 1, 1 ) == "_"
      If ( i:= (::cAlias)->( FieldPos( cMsg:= SubStr( cMsg, 2 ) ) ) ) > 0
         Return ( ::aFields[i]:= uParam1 )
      ElseIf ( i:= AScan( ::aVirtual, {|x| x[1] == cMsg } ) ) > 0
         If ValType( ::aVirtual[i][2] ) == 'B'
            Return Eval( ::aVirtual[i][2], uParam1 )
         Else
            Return ::aVirtual[i][2]:= uParam1
         EndIf
      EndIf
   Else
      if ( i:= (::cAlias)->( FieldPos( cMsg ) ) ) > 0
         Return ( ::aFields[i] )
      ElseIf ( i:= AScan( ::aVirtual, {|x| x[1] == cMsg } ) ) > 0
         If ValType( ::aVirtual[i][2] ) == 'B'
            Return Eval( ::aVirtual[i][2] )
         Else
            Return ::aVirtual[i][2]
         EndIf
      EndIf
   endif
   */
   Alert( 'Campo no encontrado ' + cMsg + ' en ' + ::cQuery )

RETURN NIL


