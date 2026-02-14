//	Copyright 2002 (c) Strat-O-Matic Game Co.
//	Knowledge of HMTL and Javascript is recommended before editing this document
//	Be sure to make backup copies before any changes
//  For use with Web Builder 1.3

headTeamScored = new Array("Runs","Games Team Scored");
headTeamAgainst = new Array("Runs","Games Opponents Scored");
headTeamDivision = new Array("Record vs. ", "Won", "Lost", "Pct");

// must match arrays created
headTeam1Bat = new Array("Name", "BAVG", "GM","AB","R","H","2B","3B","HR","RBI","BB","SO","HB","SH", "SB","CS" );
headTeam1Pitch = new Array("Name", "ERA", "W","L","PCT","G","GS","CG","SH","SV","IP","H","R","ER", "HR","BB","SO");

headTeam2Bat = new Array("Name", "PA","HR","-HM-","RD","IBB","SF","SAC","SAT","SAC%","GS","STREAK","PINCH","PAB", "PH","PHR");
headTeam2Pitch = new Array("Name", "TBF", "HRA","-HM-","RD","IBB","GF","SV","OPP","SAVE%","Hit-BAVG","AB","HIT","HR", "SAC");





function getTeam()
{
var param;
param = getParam( "team" );	

//var h = parent.location.href;
//var nr = h.split("?");
//var	vp;

//	if(( typeof(nr[1])== "undefined") )
//	{
//		param = "DEF";
//		return param;
//	}	
//	else
//	{
//		param=nr[1];
//		vp=param.split("&");
//		return vp[0];
//	}


	return param;

}  

function getTeamColor()
{
	var vTeam = "a" + getTeam();
	return  eval(vTeam)[3];
}


function DisplayRunsScrored( bgColor)
{
	// Bar Graphics for Runs for and against

	g = new Array("aRuns", "aAgainst");
	document.write("<TABLE  align=center bgColor=" + bgColor + ">");
	document.write("<TR><TD align=center>");
	BarGraph( eval(headTeamScored), eval(g[0]) );
	document.write("</TD>");

	document.write("<TD align=center>");

	BarGraph( eval(headTeamAgainst), eval(g[1]));
	document.write("</TD>");
	document.write("</TR></TABLE>");
	return;
}



//BarGraph( headArray, dataArray)
// used in Team Runs 
function BarGraph( head, data)
{
	var i;
	var rows = data.length;
	var w;
	
	document.write("<table width=295 align=center border=0 cellspacing=1 cellpadding=2 >");
	document.write("<tr>");
	document.write("<td align=left NoWrap  class='TSubHead2'>");
	document.write(head[0] + "</td>");
	document.write("<td colspan=2 align=center NoWrap  class='TSubHead2'>");
	document.write(head[1] + "</td>");
	document.write("</tr>");
	
	for(i=0;i<rows;i++)
	{
		w = data[i] * 8;
		//w = aRuns[i] * 8 ;
		r = i;
		if( r == (rows-1))
		{
			r = r + "+";
		}
		document.write("<tr>");
		document.write("<td align=center class='TData2'>");
		document.write( r + "</td>");
		document.write("<td align=left  class='TData2'>");
		document.write("<img valign=middle src='" + bargraph + "' height=10 width='" + w + "' ></td>");
		document.write("<td align=center  class='TData2'>");
		//document.write(w + "</td>");
		document.write(data[i] + "</td>");

		document.write("</tr>");
	}
	document.write("</table>");
	return;
}

function TeamDivision(div)
{

	var title = "Record vs. " + div.substring(0,2) + " " + div.substring(2);
	var Team = div + "Team";
	var Won = div + "Won";
	var	Lost = div + "Lost";
	var Pct	= div + "Pct";
	var ct = eval(Team).length;
	var i,j

	document.write("<TABLE width=295  align=center border=1 cellspacing=1 cellpadding=2 >");	
	document.write("<tr>");
	document.write("<td align=left NoWrap  class='TSubHead2'>");
	document.write(title + "</td>");
	document.write("<td  align=center NoWrap  class='TSubHead2'>");
	document.write("Won</td>");
	document.write("<td align=center NoWrap  class='TSubHead2'>");
	document.write("Lost</td>");
	document.write("<td align=center NoWrap  class='TSubHead2'>");
	document.write("Pct.</td>");
	document.write("</tr>");

	document.write("<tr>");
	document.write("<td align=left class='TData1'>");
	for(i=0;i<ct;i++)
	{
		document.write(eval(Team)[i] + "<BR>");
	}
	document.write("</td>");
	
	document.write("<td align=center class='TData2'>");
	for(i=0;i<ct;i++)
	{
		document.write(eval(Won)[i] + "<BR>");
	}
	document.write("</td>");

	document.write("<td align=center class='TData2'>");
	for(i=0;i<ct;i++)
	{
		document.write(eval(Lost)[i] + "<BR>");
	}
	document.write("</td>");

	document.write("<td align=right class='TData2'>");
	for(i=0;i<ct;i++)
	{
		document.write(eval(Pct)[i] + "<BR>");
	}
	document.write("</td>");
	document.write("</tr>");
	
	
	document.write("</table>");
	return;
}



function TeamMenu(Team, TLA)
{
	url = "team.html?" + TLA;
	url1 = "team1.html?" + TLA;
	url2 = "team2.html?" + TLA;
	
    document.write('<TABLE width=100 class="MenuText1" align=center  border="0"><TR>');
	document.write('<TD align="left">');
	document.write('<A href="' + url + '">' + Team + '</A><br><br>');
	document.write('<A href="' + url1 + '">Primary Stats</A><br><br>');
	document.write('<A href="' + url2 + '">Secondary Stats</A><br>');
	document.write('</TD>');
	document.write('</TR></TABLE>');

	return;
}


//	Grand Total Boxes
function TeamStatsTable( title, columns, headname, dataname, totalname, xsize   )
{
	var i, j;		// make sure we set this as local
	var	TLA;
	var	bg = "TData1";				
	var TeamInfo;
	var url;
	
	document.write("<table class='TTitle' border=0 cellspacing=1 cellpadding=3 width='" + xsize + "'>");
	document.write("<tr>");
	document.write("<td align=center colspan=" + columns + " class='THead'>" + title);
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
		document.write(eval(headname)[i]+ "</td>");
	}
	document.write("</tr>");


	// Draw Data column by column
	document.write("<tr>");
	for(i=0; i< columns; i++)
	{
		var vparam = dataname + i;
		var rows = eval(vparam).length;
		
		if(bg == "TData1")		// change color of each column
		{
			bg = "TData2";
		}
		else
		{
			bg = "TData1";
		}
		
		if(i==0)
		{
			document.write("<td align=left NoWrap  class='" + bg + "'>");
		}
		else
		{
			document.write("<td align=right NoWrap class='" + bg + "'>");
		}
		
		for(j=0; j < rows; j++)
		{
			document.write( eval(vparam)[j] );
			document.write("<BR>");
		}	
		document.write("</td>");
	}
	document.write("</tr>");
	

	// Draw Totals row				
	if(typeof(eval(totalname)[0]) != "undefined")
	{
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
			document.write(eval(totalname)[i]+ "</td>");
		}
		document.write("</tr>");
	}
	
	
	document.write("</table>");
	return;
}




//	Grand Total Boxes
function TeamLeaderTable( title, columns, headname, dataname, pType,  xsize, iSortColumn, bDirection, iTable   )
{
	var i
	var j;		// make sure we set this as local
	var k;
	var	TLA;
	var	bg = "TData1";				
	var TeamInfo;
	var url;
	var vparam;
	var iNext=0;
	var iMatch=0;
	var rows;
	var vparam;
	dSort = new Array(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);
	
		
	document.write("<table class='TTitle' border=0 cellspacing=1 cellpadding=3 width='" + xsize + "'>");
	document.write("<tr>");
	document.write("<td align=center colspan=" + columns + " class='THead'>" + title);
	document.write("</td></tr>");
	
	
	// Draw Header row				
	document.write("<tr>");
	for(i=0; i< columns; i++)
	{
		if(i==0)
		{
			document.write("<td align=left NoWrap  class='TSubHead2'>");
			document.write("<A class='TSubHead2' href='" + TeamSortUrl( 0,iTable ) + "'>");
			document.write("Name</A></td>");
		}
		else
		{
			document.write("<td align=center NoWrap  class='TSubHead2'>");
			document.write("<A class='TSubHead2' href='" + TeamSortUrl( i,iTable ) + "'>");
			document.write(eval(headname)[i]+ "</A></td>");
		}
	}
	document.write("</tr>");


	// sort routine
	if(iSortColumn > -1 )
	{
		vparam = dataname + iSortColumn;
		rows = eval(vparam).length;
		for(i=0; i< rows; i++)
					dSort[i] = 0;
			
		for(i=0; i< rows; i++)
		{
			iMatch = 0;
			for( j=0; j < rows; j++)
			{
				if(bDirection)
				{
					// Version 7.01 - added following:
					if (iSortColumn == 0)
					{
						t2 = eval(vparam)[i].substring(2,15);
						t1 =  eval(vparam)[dSort[j]].substring(2,15);
					}
					else
					{
						t1 = eval(vparam)[i]-0;
						t2 =  eval(vparam)[dSort[j]]-0;
					}
				}
				else
				{
					t2 = eval(vparam)[i]-0;
					t1 =  eval(vparam)[dSort[j]]-0;
				}

				if( t1 >  t2 )
				{
					iMatch = 1;
					for(k =  rows; j <= k; k--)
					{
						dSort[k+1] = dSort[k];
					}
					dSort[j] = i;
					iNext++;
					break;
				}
			}	
			
			if(iMatch == 0)
			{
				dSort[iNext++] = i;
			}
		}
	}




	// Draw Data column by column
	document.write("<tr>");
	for(i=0; i< columns; i++)
	{
		vparam = dataname + i;
		rows = eval(vparam).length;
					
		if(bg == "TData1")		// change color of each column
		{
			bg = "TData2";
		}
		else
		{
			bg = "TData1";
		}
		
		if(i==0)
		{
			document.write("<td align=left NoWrap  class='" + bg + "'>");
		}
		else
		{
			document.write("<td align=center NoWrap class='" + bg + "'>");
		}
		
		for(j=0; j < rows; j++)
		{
			if(i==0)
			{	// column =1 which is expected to be the team name
				var pName = DisplayPlayerURL( eval(vparam)[dSort[j]], getTeam(), pType );
				document.write( pName );

			}
			else
			{	
				document.write( eval(vparam)[dSort[j]] );
			}
			document.write("<BR>");
		}	
		document.write("</td>");
	}
	document.write("</tr>");
	

	document.write("</table>");
	return;
}
