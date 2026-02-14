//	Copyright 2002 (c) Strat-O-Matic Game Co.
//	Knowledge of HMTL and Javascript is recommended before editing this document
//	Be sure to make backup copies before any changes
//  For use with Web Builder 1.1 
headPlayerB1 = new Array("Name","Average","AB","Hits","2B","3B","HR","RBI","BB","IBB","SO","PAB","PH","PHR" );
headPlayerB2 = new Array("Name","Games","PA","Sac Hits","Sac %","HBP", "SB","CS","GIDP","Slug%","OB%","OB+SL","Hit Stk","Errors" );
b2Index = new Array( 5, 6, 17, 19, 21, 25, 26, 28, 29, 30, 31, 29, 76 );
b1Index = new Array( 10, 7, 9, 13, 14, 15, 16, 22, 23, 24, 45, 46, 48 );
b1leftIndex = new Array( 51, 49, 50, 52,53,54, 55,56,57,58, 59,60, 61 );
b1rightIndex = new Array( 64, 62, 63, 65, 66, 67, 68, 69, 70, 71,72,73,74 );

headPlayerP1 = new Array("Pitcher","AB","Hits","OpAvg","HR","BB","IBB","K","OBFP" );
p1Index = new Array( 20, 21, 22, 23, 24, 25, 26, 27 );
p1leftIndex = new Array( 50, 51, 52, 53,54,55, 56,57 );
p1rightIndex = new Array( 58, 59, 60, 61, 62, 63, 64, 65 );


function B1PlayerDetailTable( name )
{
	var i;	
	var	bg = "PlayerData1";				
	columns = 14;
	
	document.write("<table  bgColor=white  class='TTitle' border=1 >");
	
	// Draw Header row				
	document.write("<tr>");
	for(i=0; i< columns; i++)
	{
		if(i==0)
		{
			document.write("<td align=left NoWrap  class='TSubHead2'>");
		}
		else
		{
			document.write("<td align=center NoWrap  class='TSubHead2'>");
		}
		document.write( headPlayerB1[i] + "</td>");
	}
	document.write("</tr>");

	// Draw Player Details
	document.write("<tr>");
	document.write("<td align=left NoWrap  class='PlayerData2'>");
	document.write( eval(name)[89] + "<br>" );
	document.write( "versus Left<br>" );
	document.write( "versus Right");
	document.write("</td>");

	for(i=0; i< columns-1; i++)
	{
		document.write("<td align=center NoWrap class='" + bg + "'>");
		// write Column Data			
		document.write( eval(name)[b1Index[i]] + "<br>" );
		document.write( eval(name)[b1leftIndex[i]] + "<br>" );
		document.write( eval(name)[b1rightIndex[i]] );
		document.write("</td>");
	}
	document.write("</tr>");
	document.write("</table>");
	return;
}

function B2PlayerDetailTable( name )
{
	var i;	
	var	bg = "PlayerData1";				
	columns = 12;
	
	document.write("<table  bgColor=white  class='TTitle' border=1 >");
	
	// Draw Header row				
	document.write("<tr>");
	for(i=0; i< columns; i++)
	{
		if(i==0)
		{
			document.write("<td align=left NoWrap  class='TSubHead2'>");
		}
		else
		{
			document.write("<td align=center NoWrap  class='TSubHead2'>");
		}
		document.write( headPlayerB2[i] + "</td>");
	}
	document.write("</tr>");

	// Draw Player Details
	document.write("<tr>");
	document.write("<td align=left NoWrap  class='PlayerData2'>");
	document.write( eval(name)[89] );
	document.write("</td>");

	for(i=0; i< columns-1; i++)
	{
		document.write("<td align=center NoWrap class='" + bg + "'>");
		// write Column Data			
		document.write( eval(name)[b2Index[i]]);
		document.write("</td>");
	}
	document.write("</tr>");
	document.write("</table>");
	return;
}




function PPlayerDetailTable( name )
{
	var i;	
	var	bg = "PlayerData1";				
	var	title;
	
	title = "Details"
	if(pitcher[3] == "R")
		title = "Throws Right";
	if(pitcher[3] == "L")
			title = "Throws Left";
	columns = 9;
	
	document.write("<table  bgColor=white  class='TTitle' border=2 >");
	document.write("<tr>");
	document.write("<td align=left colspan=9 class='THead'>" + title);
	document.write("</td></tr>");
	// Draw Header row				
	document.write("<tr>");
	for(i=0; i< columns; i++)
	{
		if(i==0)
		{
			document.write("<td align=left NoWrap  class='TSubHead2'>");
		}
		else
		{
			document.write("<td align=center NoWrap  class='TSubHead2'>");
		}
		document.write( headPlayerP1[i] + "</td>");
	}
	document.write("</tr>");

	// Draw Player Details
	document.write("<tr>");
	document.write("<td align=left NoWrap  class='PlayerData2'>");
	document.write( eval(name)[79] + "<br>" );
	document.write( "versus Left<br>" );
	document.write( "versus Right");
	document.write("</td>");

	for(i=0; i< columns-1; i++)
	{
		document.write("<td align=center NoWrap class='" + bg + "'>");
		// write Column Data			
		document.write( eval(name)[p1Index[i]] + "<br>" );
		document.write( eval(name)[p1leftIndex[i]] + "<br>" );
		document.write( eval(name)[p1rightIndex[i]] );
		document.write("</td>");
	}
	document.write("</tr>");
	document.write("</table>");
	return;
}


// style 0 = Actual parameter
// style 1 = Period removed and everything is lower case
// style 2 = Period replaced with space
function getPlayerName(style)
{
	var h = parent.location.href;
	var nr = h.split("?");
	var param;
	var	vp;
	var result;
	var	f;
	var lcName 

	if(( typeof(nr[1])== "undefined") )
	{
		param = "D.efPlayer";
	}	
	else
	{
		param=nr[1];
		vp=param.split(",");
		param = vp[0];
	}
	
	result = param;
	switch(style)
	{
		case 1:
			lcName = param.toLowerCase();
			f = lcName.split(".");
			result = f[0] + f[1];
			break;
		case 2:
			f = param.split(".");
			result = f[0] + ". " + f[1];
			break;
		default:
			result = param;
			break;
	}		
	
	return result;
	
}  


