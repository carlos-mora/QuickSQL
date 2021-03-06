#include "hbclass.ch"
#include "common.ch"
#include "dbstruct.ch"
#include "set.ch"
#include "mysql.ch"

#translate ASSTRING( <x> ) => If( <x> == NIL, 'NIL', Transform( <x> , NIL ) )
#xcommand DEBUG <cString1>[, <cStringN>] ;
         => ;
          WAPI_OutputDebugString( ProcName() +"("+LTrim(Str(ProcLine())) +") - " + <"cString1"> + " ("+ValType( <cString1> )+"): " + ASSTRING( <cString1> ) + HB_OSNewLine() ) ;
          [ ; WAPI_OutputDebugString( ProcName() +"("+LTrim(Str(ProcLine())) +") - " + <"cStringN">+" ("+ValType( <cStringN> )+"): " + ASSTRING( <cStringN> ) + HB_OSNewLine() ) ]


CLASS TModelBase

   VAR   oConnection    INIT NIL
   VAR   cTable         INIT NIL
   VAR   aRecord        INIT NIL
   VAR   aOriginal      INIT NIL
   VAR   aSchema        INIT NIL
   VAR   aFields        INIT NIL
   VAR   nPKIndex       INIT 0
   VAR   cPKFieldName   INIT 'id'
   VAR   lChanged       INIT .F.
   VAR   lError         INIT .F.


   METHOD   New( oConnection, cTableName, aDefinition )
   METHOD   End()
   DESTRUCTOR Destroy()

   METHOD   GetSchema()
   METHOD   GetById( id )
   METHOD   GetWhere( cField, cValue, ... )
   METHOD   GetRow( cWhere )
   METHOD   Update()

   MESSAGE  Replace() METHOD Update()
   MESSAGE  Save() METHOD Update()

   METHOD   Delete()
   METHOD   Append()
   METHOD   Blank()
   METHOD   FromWorkArea()

   METHOD   FieldPut( nField, Value )
   METHOD   Refresh()
   METHOD   MakePrimaryKeyWhere()    // returns a WHERE x=y statement which uses primary key (if available)

   ERROR HANDLER OnError( uParam1 )


ENDCLASS


METHOD New( oConnection, cTableName, aFields ) CLASS TModelBase
   LOCAL i

   ::oConnection := oConnection
   ::cTable := cTableName
   ::aFields := aFields

   IF Empty( ::aSchema )
      ::GetSchema()
   ENDIF
   IF Empty( ::aSchema )
      // Error
      IF ValType( aFields ) == 'A'

      ENDIF
   ELSE

      IF (::nPKIndex:= aScan( ::aSchema, {|x| HB_BitTest( x[MYSQL_FS_FLAGS], PRI_KEY_BIT ) } ) ) > 0
         ::cPKFieldName:= ::aSchema[::nPKIndex][ MYSQL_FS_NAME ]
      ELSE

      ENDIF
   ENDIF

RETURN Self

//------------------------------------------------------------------------------
METHOD End()
//------------------------------------------------------------------------------
RETURN NIL

//------------------------------------------------------------------------------
METHOD Destroy()
//------------------------------------------------------------------------------
RETURN ::End()

//------------------------------------------------------------------------------
METHOD GetById( id ) CLASS TModelBase
//------------------------------------------------------------------------------
   LOCAL oQuery, aRow

   oQuery:= ::oConnection:Query( 'SELECT * FROM `' + ::cTable + '` WHERE ' + ::cPKFieldName + '=' + ClipValue2SQL( id ) + ' LIMIT 1' )

   IF oQuery:LastRec() == 1

      aEval( aRow:= mysql_fetch_row( oQuery:hResult ), {|x,i| aRow[i] := SqlValue2Clip( aRow[i], ::aSchema[i][MYSQL_FS_TYPE] ) } )

      ::aOriginal:= aClone( ::aRecord:= aRow )

      RETURN .T.
   ENDIF

   ::aRecord:= ::aOriginal:= NIL

RETURN .F.

//------------------------------------------------------------------------------
METHOD GetWhere( cField, cValue, ... ) CLASS TModelBase
//------------------------------------------------------------------------------
   LOCAL cWhere, i
   IF ValType( cField ) == 'A'
      cWhere:= ArrayAsList( AEval( cField, {|x,i| cField[i] := __ConcatFieldValue( x, ClipValue2SQL( cValue[i] ) ) } ) , ' AND ' )
   ELSE
      IF PCount() == 2 .AND. !( '$1' $ cField )  // field value pair, and ! parametrized exp
         cWhere:= __ConcatFieldValue( cField, ClipValue2SQL( cValue ) )
      ELSE
         i:= PCount()
         cWhere := cField
         // Replacing query params from last to first. Avoids repeating HB_Param calls in iteration,
         // and the problem of having more than 10 params givin $10 get replaced by $1
         WHILE i > 1
            cWhere:= StrTran( cWhere, '$'+(LTrim(Str(i-1))), ClipValue2SQL( HB_PValue(i) ) )
            i--
         ENDDO
      ENDIF
   ENDIF

RETURN ::GetRow( cWhere )

STATIC FUNCTION __ConcatFieldValue( cField, cValue )
   IF '=' $ cField .OR. '>' $ cField .OR. '<' $ cField
      RETURN cField + cValue
   ENDIF
RETURN cField + '=' + cValue

//------------------------------------------------------------------------------
METHOD GetRow( cWhere ) CLASS TModelBase
//------------------------------------------------------------------------------
   LOCAL oQuery, aRow

   oQuery:= ::oConnection:Query( 'SELECT * FROM `' + ::cTable + '` WHERE ' + cWhere + ' LIMIT 1' )
   IF oQuery:LastRec() == 1
      aEval( aRow:= mysql_fetch_row( oQuery:hResult ), {|x,i| aRow[i] := SqlValue2Clip( aRow[i], ::aSchema[i][MYSQL_FS_TYPE] ) } )

      ::aOriginal:= aClone( ::aRecord:= aRow )

      RETURN .T.

   ENDIF

   ::aRecord:= ::aOriginal:= NIL

RETURN .F.

//------------------------------------------------------------------------------
METHOD GetSchema() CLASS TModelBase
//------------------------------------------------------------------------------
   ::aSchema:= ::oConnection:Query( 'SELECT * FROM `'+ ::cTable +'` LIMIT 0' ):aMetaData
RETURN Self

//------------------------------------------------------------------------------
METHOD OnError( uParam1 )
//------------------------------------------------------------------------------
   LOCAL cMsg   := __GetMessage()
   LOCAL i

   IF Left( cMsg, 1 ) == "_"

      cMsg:= SubStr( cMsg, 2 )

      IF ( i:= AScan( ::aSchema, {|x| Upper(x[MYSQL_FS_NAME]) == cMsg } ) ) > 0
         RETURN ::aRecord[i]:= uParam1
      ENDIF

      // buscar relaciones
   ELSE

      IF ( i:= AScan( ::aSchema, {|x| Upper(x[MYSQL_FS_NAME]) == cMsg } ) ) > 0
         RETURN ::aRecord[i]
      ENDIF

      // buscar relaciones
   ENDIF

   Alert( 'Campo no encontrado ' + cMsg + ' en ' + ::cTable )

RETURN NIL


//------------------------------------------------------------------------------
METHOD FromWorkArea() CLASS TModelBase
//------------------------------------------------------------------------------
   LOCAL i, j

   IF !Empty( Alias() )

      // TODO: VErificar que estamos en un registro donde hacer reemplazos

      FOR i:= 1 TO FCount()
         IF ( j:= AScan( ::aSchema, {|x| Upper(x[MYSQL_FS_NAME]) == Field(i) } ) ) > 0
            ::aRecord[j]:= FieldGet( i )
         ENDIF
      NEXT
   ENDIF

RETURN Self
/* Creates an update query for changed fields and submits it to server */

//------------------------------------------------------------------------------
METHOD Update() CLASS TModelBase
//------------------------------------------------------------------------------

   LOCAL cQuery, oQuery
   LOCAL i
   LOCAL aFields, aValues

   aFields:= {}
   aValues:= {}
   ::lError := .F.

   IF ::aOriginal == NIL
      // New Record
      // INSERT INTO `` () VALUES ()
      FOR i := 1 TO Len( ::aSchema )
         IF (i != ::nPKIndex) .AND. !(::aRecord[i] == BlankValue( ::aSchema[i][MYSQL_FS_TYPE]))
            aAdd( aFields, '`' + ::aSchema[i][MYSQL_FS_NAME] + '`' )
            aAdd( aValues, ClipValue2SQL( ::aRecord[i] ) )
         ENDIF
      NEXT

      IF Len( aFields ) > 0
         cQuery:= 'INSERT INTO `' + ::cTable + '` ('+ ArrayAsList( aFields, ',' ) + ') VALUES (' + ArrayAsList( aValues, ',' ) + ')'
      ENDIF

      IF (::lError:= !::oConnection:Execute( cQuery ) )
      ELSE
         // ::GetByID( ::oConnection:InsertId() )
         ::aRecord[ ::nPKIndex ] := ::oConnection:InsertId()
      ENDIF

   ELSE

      FOR i := 1 TO Len( ::aSchema )
         // http://dev.mysql.com/doc/refman/5.5/en/server-sql-mode.html#sqlmode_pad_char_to_full_length
         // MySQL by default trims white spaces at the end of strings, so keeping that in mind...
         IF !( ::aOriginal[i] == IF( ValType( ::aRecord[i] ) == 'C', RTrim(::aRecord[i]), ::aRecord[i] ) )
            aAdd( aFields, '`' + ::aSchema[i][MYSQL_FS_NAME] + '` = ' + ClipValue2SQL( ::aRecord[i] ) )
         ENDIF
      NEXT

      IF Len( aFields ) > 0
         cQuery:= 'UPDATE `' + ::cTable + '` SET '+ ArrayAsList( aFields, ' , ' ) + ::MakePrimaryKeyWhere() + ' LIMIT 1'
         ::lError:= !::oConnection:Execute( cQuery )
      ELSE
         ::lError:= .F.
      ENDIF

   ENDIF

   RETURN !::lError


//------------------------------------------------------------------------------
METHOD Delete() CLASS TModelBase
//------------------------------------------------------------------------------

   IF ::aOriginal == NIL // Es un blank
      ::lError:= .F.
   ELSE
      // cDeleteQuery := "DELETE FROM " + ::cTable + ' ' + ::MakePrimaryKeyWhere() + ' LIMIT 1'
      ::lError:= !::oConnection:Execute( 'DELETE FROM ' + ::cTable + ' ' + ::MakePrimaryKeyWhere() + ' LIMIT 1' )
   ENDIF
   ::aRecord:= ::aOriginal:= NIL

   RETURN !::lError


//------------------------------------------------------------------------------
METHOD Append() CLASS TModelBase // clonar el registro actual: limpiar id y aoriginal ...
//------------------------------------------------------------------------------

   LOCAL cInsertQuery := "INSERT INTO " + ::cTable + " ("
   LOCAL i
   LOCAL oRow, lRefresh


   DEFAULT lRefresh TO .T.

   IF oRow == NIL // default Current row

      // field names
      FOR i := 1 TO ::nNumFields
         IF ::aSchema[i][MYSQL_FS_FLAGS] != AUTO_INCREMENT_FLAG
            cInsertQuery += ::aMetaData[i][MYSQL_FS_NAME] + ","
         ENDIF
      NEXT
      // remove last comma from list
      cInsertQuery := Left( cInsertQuery, Len( cInsertQuery ) - 1 ) + ") VALUES ("

      // field values
      FOR i := 1 TO ::nNumFields
         IF ::aMetaData[i][MYSQL_FS_FLAGS] != AUTO_INCREMENT_FLAG
            // cInsertQuery += ClipValue2SQL( ::FieldGet( i ) ) + ","
         ENDIF
      NEXT

      // remove last comma from list of values and add closing parenthesis
      cInsertQuery := Left( cInsertQuery, Len( cInsertQuery ) - 1 ) + ")"

      IF mysql_query( ::nSocket, cInsertQuery ) == 0
         ::lError := .F.

         ::nCurRow := ::lastrec() + 1

         IF lRefresh
            ::refresh()
         ELSE
            /* was same values from fieldget( i ) !
            FOR i := 1 TO ::nNumFields
                ::aOldValue[i] := ::FieldGet( i )
            NEXT
            */
         ENDIF

         RETURN .T.
      ELSE
         ::lError := .T.
      ENDIF

   ELSE

      IF oRow:cTable == ::cTable

         // field names
         FOR i := 1 TO Len( oRow:aRow )
            IF oRow:aMetaData[i][MYSQL_FS_FLAGS] != AUTO_INCREMENT_FLAG
               cInsertQuery += oRow:aMetaData[i][MYSQL_FS_NAME] + ","
            ENDIF
         NEXT
         // remove last comma from list
         cInsertQuery := Left( cInsertQuery, Len( cInsertQuery ) - 1 ) + ") VALUES ("

         // field values
         FOR i := 1 TO Len( oRow:aRow )
            IF oRow:aMetaData[i][MYSQL_FS_FLAGS] != AUTO_INCREMENT_FLAG
               // cInsertQuery += ClipValue2SQL( oRow:aRow[i] ) + ","
            ENDIF
         NEXT

         // remove last comma from list of values and add closing parenthesis
         cInsertQuery := Left( cInsertQuery, Len( cInsertQuery ) - 1 ) + ")"

         IF mysql_query( ::nSocket, cInsertQuery ) == 0
            ::lError := .F.

            RETURN .T.
         ELSE
            ::lError := .T.
         ENDIF
      ENDIF

   ENDIF

RETURN .F.


METHOD Blank() CLASS TModelBase

   LOCAL i
   LOCAL aRow := Array( Len(::aSchema ) )

   // crate an array of empty fields
   FOR i := 1 TO Len(::aSchema )
      aRow[i] := BlankValue( ::aSchema[i][MYSQL_FS_TYPE] )

      /*
      SWITCH ::aSchema[i][MYSQL_FS_TYPE]
      CASE MYSQL_STRING_TYPE
      CASE MYSQL_VAR_STRING_TYPE
      CASE MYSQL_BLOB_TYPE
      CASE MYSQL_DATETIME_TYPE
         aRow[i] := ""
         EXIT

      CASE MYSQL_SHORT_TYPE
      CASE MYSQL_LONG_TYPE
      CASE MYSQL_LONGLONG_TYPE
      CASE MYSQL_INT24_TYPE
      CASE MYSQL_DECIMAL_TYPE
         aRow[i] := 0
         EXIT

      CASE MYSQL_TINY_TYPE
         aRow[i] := .F.
         EXIT

      CASE MYSQL_DOUBLE_TYPE
      CASE MYSQL_FLOAT_TYPE
         aRow[i] := 0.0
         EXIT

      CASE MYSQL_DATE_TYPE
         aRow[i] := hb_SToD( "" )
         EXIT

      OTHERWISE
         aRow[i] := NIL

      ENDSWITCH
      */

   NEXT
   ::aRecord  := aRow
   ::aOriginal:= NIL

RETURN Self

METHOD FieldPut( cnField, Value ) CLASS TModelBase

   LOCAL nNum

   IF ISCHARACTER( cnField )
      nNum := ::FieldPos( cnField )
   ELSE
      nNum := cnField
   ENDIF

   IF nNum > 0 .AND. nNum <= ::nNumFields

      IF Valtype( Value ) == Valtype( ::aRow[nNum] ) .OR. ::aRow[nNum] == NIL

         // if it is a char field remove trailing spaces
         IF ISCHARACTER( Value )
            Value := RTrim( Value )
         ENDIF

         //DAVID:
         ::aRow[ nNum ] := Value

         RETURN Value
      ENDIF
   ENDIF

   RETURN NIL


METHOD Refresh()

   // free present result handle
   mysql_free_result( ::nResultHandle )

   ::lError := .F.

   IF mysql_query( ::nSocket, ::cQuery ) == 0

      // save result set
      ::nResultHandle := mysql_store_result( ::nSocket )
      ::nNumRows := mysql_num_rows( ::nResultHandle )

      // NOTE: I presume that number of fields doesn't change (that is nobody alters this table) between
      // successive refreshes of the same

      // But row number could very well change
      IF ::nCurRow > ::nNumRows
         ::nCurRow := ::nNumRows
      ENDIF

      ::getRow( ::nCurRow )

   ELSE
/*      ::aMetaData := {}
      ::nResultHandle := NIL
      ::nNumFields := 0
      ::nNumRows := 0

      ::aOldValue := {}
      */
      ::lError := .T.
   ENDIF

   RETURN !::lError


// returns a WHERE x=y statement which uses primary key (if available)
METHOD MakePrimaryKeyWhere()

   LOCAL ni
   LOCAL aWhere:= {}

   FOR ni := 1 TO Len( ::aSchema )
      // search for fields part of a primary key
      IF hb_bitAnd( ::aSchema[ni][MYSQL_FS_FLAGS], PRI_KEY_FLAG ) == PRI_KEY_FLAG .OR.;
         hb_bitAnd( ::aSchema[ni][MYSQL_FS_FLAGS], MULTIPLE_KEY_FLAG ) == MULTIPLE_KEY_FLAG

         aAdd( aWhere, ::aSchema[ni][MYSQL_FS_NAME] + "=" + ClipValue2SQL( ::aOriginal[ni] ) )

      ENDIF

   NEXT

   RETURN " WHERE " + ArrayAsList( aWhere, " AND " )

FUNCTION BlankValue( nSqlType )

      SWITCH nSqlType
      CASE MYSQL_STRING_TYPE
      CASE MYSQL_VAR_STRING_TYPE
      CASE MYSQL_BLOB_TYPE
      CASE MYSQL_DATETIME_TYPE
         RETURN ""
         EXIT

      CASE MYSQL_SHORT_TYPE
      CASE MYSQL_LONG_TYPE
      CASE MYSQL_LONGLONG_TYPE
      CASE MYSQL_INT24_TYPE
      CASE MYSQL_DECIMAL_TYPE
         RETURN 0
         EXIT

      CASE MYSQL_TINY_TYPE
         RETURN .F.
         EXIT

      CASE MYSQL_DOUBLE_TYPE
      CASE MYSQL_FLOAT_TYPE
         RETURN 0.0
         EXIT

      CASE MYSQL_DATE_TYPE
         RETURN hb_SToD( "" )
         EXIT

      ENDSWITCH

RETURN NIL

FUNCTION ClipValue2SQL( uValue )

   LOCAL cValue

   IF uValue == NIL
      RETURN 'NULL'
   ENDIF

   SWITCH ValType( uValue )
   CASE "N"
      cValue := hb_NToS( uValue )
      EXIT

   CASE "D"
      IF Empty( uValue )
         cValue := "''"
      ELSE
         cValue := "'" + StrZero( Year( uValue ), 4 ) + "-" + StrZero( Month( uValue ), 2 ) + "-" + StrZero( Day( uValue ), 2 ) + "'"
      ENDIF
      EXIT

   CASE "C"
   CASE "M"
      cValue := "'" + mysql_escape_string( uValue ) + "'"
      EXIT

   CASE "L"
      cValue := iif( uValue, "1", "0" )
      EXIT

   OTHERWISE
      cValue := "''"       // NOTE: Here we lose values we cannot convert

   ENDSWITCH

   RETURN cValue

//---------------------------------------------------------------------------------
FUNCTION ArrayAsList( aArray, cDelimiter, lTrim )
//---------------------------------------------------------------------------------

   LOCAL i, nLen              // Position of cDelimiter in cList
   LOCAL cList

   If cDelimiter == NIL
      cDelimiter := ","
   EndIf

   If ValType( lTrim ) != 'L'
      lTrim:= .F.
   EndIf

   If ( nLen:= Len( aArray ) ) == 0
      Return ''
   EndIf

   cList := If( lTrim, AllTrim( aArray[1] ), aArray[1] )

   For i:= 2 To nLen
      cList+= cDelimiter + If( lTrim, AllTrim( aArray[i] ), aArray[i] )
   EndFor

   RETURN ( cList )                             // Return the array

//---------------------------------------------------------------------------------
FUNCTION ListAsArray( cList, cDelimiter, lTrim )
//---------------------------------------------------------------------------------

   LOCAL nPos              // Position of cDelimiter in cList
   LOCAL aList := {}       // Define an empty array

   If cDelimiter == NIL
      cDelimiter := ","
   EndIf

   If ValType( lTrim ) != 'L'
      lTrim:= .F.
   EndIf

   If lTrim
      cList:= AllTrim( cList )
   EndIf

   If Empty( cList )
      Return {}
   EndIf
   // Loop while there are more items to extract
   DO WHILE ( nPos := AT( cDelimiter, cList )) != 0

      // Add the item to aList and remove it from cList
      AADD( aList, If( lTrim, AllTrim( SUBSTR( cList, 1, nPos - 1 ) ), SUBSTR( cList, 1, nPos - 1 ) ) )
      cList := SUBSTR( cList, nPos + Len( cDelimiter ) ) // Changed +1 To +Len( cDelimiter ) to allow separators like CRLF

   ENDDO
   AADD( aList, If( lTrim, AllTrim( cList ), cList ) ) // Add final element

   RETURN ( aList )                             // Return the array

