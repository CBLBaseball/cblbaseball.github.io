//	Copyright 2002 (c) Strat-O-Matic Game Co.
//	Knowledge of HMTL and Javascript is recommended before editing this document
//	Be sure to make backup copies before any changes
//  For use with Web Builder 1.3 


headStandings = new Array("Division", "Wins", "Losses", " GB ", "Win Pct.", "Home Pct.", "Road Pct.");


headLeaders = new Array ("Batting Average", "At Bats", "Runs Scored", "Hits",
						"Doubles", "Triples", "Home Runs", "Runs Batted In",
						"Walks", "Intentional Walks", "Strike Outs", "Hit By Pitch",
						"Sacrifice Hits", "Stolen Bases", "Stolen Base Pct", "GIDP",
						"Hitting Streak", "Pinch Hit Bat Ave", "Slugging Pct", "On Base Pct",
						"Runs Created/27Out", "Total Average", "Total Bases", "Bat Avg vs. Left",
						"HR vs. Left", "Bat Avg vs. Right", "HR vs. Right",
						"Wins", "Losses","Winning Pct","ERA","Innings Pitched", "TBF",
						"Games Pitched", "Games Started", "Complete Games", "Games Finished",
						"Saves", "Save Pct", "ShutOuts", "Hits Allowed", "Runs",
						"Earned Runs", "Home Runs Allowed", "Walks", "Strikeouts",
						"Wild Pitches", "Balks", "Pitcher X-Pct", "Opponent Steals",
						"Opponent SB Pct", "Pitchers'S Bat Avg", "Pitchers'S HRs",
						"Hits / 9 Innings", "BB / 9 Innings","Ks / 9 Innings","HRs / 9 Innings" );


											
function BoxLeaders(iNum, leaderNum )
{
	var Title = headLeaders[leaderNum];
	var Name = "leaderName" + leaderNum;
	var Team = "leaderTeam" + leaderNum;
	var	Data = "leaderData" + leaderNum;
	var	pType = 0;		// 0 = batter, 1 = pitcher
	
	if( leaderNum > 57)	return;
	
	if(typeof( eval(Data)[0]) == "undefined" )
					return;


	// Title Row
	document.write("<table align=center border=0 cellspacing=0 cellpadding=0>");
	document.write("<tr><td class='BoxHead3' colspan=4 align=center nowrap>");
	document.write(Title + "</td>");
	document.write("</tr>");
	

	//	Player Data one column at a time
	document.write("<tr>");
		//	Column 1  Ordinal Number
	document.write("<td align=right NoWrap class='TData0'>");
	for( i=0; i < iNum;i++)
	{
		document.write(i+1 + ".<br>");
	}

	//	Column 2	Player Name
	document.write("</td>");
	document.write("<td align=left NoWrap class='TData1'>");
	for( i=0; i < iNum;i++)
	{
		if(leaderNum > 26)
			pType = 1;
		else
			pType = 0;
		var pName = DisplayPlayerURL( eval(Name)[i], eval(Team)[i], pType );
		
		document.write( pName  + "<br>");
	}
	document.write("</td>");

	//	Column 3	Team
	document.write("<td align=left NoWrap class='TData1'>");
	//	Name = "leaderName" + leaderNum;
	for( i=0; i < iNum;i++)
	{
		document.write(DisplayTeamURL( eval(Team)[i] ) + "<br>");
	}
	document.write("</td>");
	
	//	Column 4	Data
	document.write("<td align=left NoWrap class='TData1'>");
	for( i=0; i < iNum;i++)
	{
		document.write(eval(Data)[i] + "<br>");
	}
	document.write("</td>");
	document.write("</tr>");
	// force maximum size of table
	document.write("<tr><td colspan=4 align=center nowrap>");
	document.write("<img src='pixel.gif' width=150 height=1 alt='League Leaders'> </td>");
	document.write("</tr>");

	
	
	document.write("</table>");
	return;
}



function BoxStandings(sDivTitle, aDiv, columns)
{
	var i;		// make sure we set this as local
	var	j;
	var k = 0;
	var	pos;	//	alignment
	var	bg = "TData2";				
	var url;
	dArray = new Array( 1,6,7,8,9,10 );		// display columns
	dSort = new Array(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);
	sortKeys = new Array();
	sortIndx = new Array();
	var	t1;
	var t2;
	var team1, team2;
	var tCount;
	var iNext=0;
	var iMatch = 0;
	var iGamesFirst = 0;
	var GamesBack = 0;
	var iGamesUp = 0;
	var sortCol = 7;
	var totWins;
	var totLoss;
						
	// Division Row
	document.write("<tr>");
	document.write("<td align=left NoWrap  class='TSubHead2'>");
	document.write( sDivTitle + "</td>");
	for(i=1; i< columns+1; i++)
	{
		document.write("<td align=center NoWrap  class='TSubHead2'>");
		document.write(headStandings[i]+ "</td>");
	}
	document.write("</tr>");


	tCount = eval(aDiv).length;
	
	// sort routine

	// Set up sort keys.
	for (i = 0; i < tCount; i++)
	{
		team1 = eval(aDiv)[i];
		totWins = eval(team1)[6] - 0;
		totLoss = eval(team1)[7] - 0;

		sortIndx[i] = i;
		sortKeys[i] = ((totWins + totLoss) == 0) ? .4999 : totWins / (totWins + totLoss);
	}

	for (i = 0; i < tCount; i++)
	{
		for (j = i + 1; j < tCount; j++)
		{
			team1 = eval(eval(aDiv)[sortIndx[i]]);
			team2 = eval(eval(aDiv)[sortIndx[j]]);
			
			if (sortKeys[j] >= sortKeys[i])
			{
				if (sortKeys[i] == sortKeys[j])
				{
					if (sortKeys[i] > .5000)
					{
						// Tiebreaker is games played.
						if (((eval(team2)[6] - 0) + (eval(team2)[7] - 0)) < ((eval(team1)[6] - 0) + (eval(team1)[7] - 0)))
						{
							continue;
						}
					}
					else
					{
						// Tiebreaker is least games played.
						if (((eval(team2)[6] - 0) + (eval(team2)[7] - 0)) > ((eval(team1)[6] - 0) + (eval(team1)[7] - 0)))
						{
							continue;
						}
					}
				}
				
				// Swap.
				k = sortIndx[i];
				sortIndx[i] = sortIndx[j];
				sortIndx[j] = k;
				k = sortKeys[i];
				sortKeys[i] = sortKeys[j];
				sortKeys[j] = k;
			}
		}
	}
	
	// One row for each team	
	for (i = 0; i < tCount; i++)
	{
		bg = (bg == "TData1") ? "TData2" : "TData1";

		document.write("<tr>");
		team1 = eval(aDiv)[sortIndx[i]];
		
		for (j = 0; j < columns; j++)
		{	
			if(j==0)			// position column 0 as left, otherwise center
			{
				pos = "left";
				url=  "team.html?team=" + team1;
				document.write("<td  NoWrap align='" + pos +  "' class='" + bg +  "'>");
				document.write("<A class='" + bg +  "' href='" + url + "'>");
				document.write(eval(team1)[dArray[j]] + "</A></TD>");

			}
			else
			{
				pos = "center";
				if(j == 3 )		// Special case for games back of leader
				{				// wins - losses
					iGamesUp = eval(team1)[dArray[1]] - eval(team1)[dArray[2]];
	
					if(i==0)
					{
						iGamesFirst = iGamesUp;
						// Version 7.1 changed:
						GamesBack = "-";
					}
					else
					{
						GamesBack = (iGamesFirst - iGamesUp) / 2;
						// Version 7.1 added:
						if (GamesBack == 0)
							GamesBack = "-";
					}
					document.write("<td  NoWrap align='" + pos +  "' class='" + bg +  "'>");
					document.write(GamesBack + "</TD>");	
				}
				
				document.write("<td  NoWrap align='" + pos +  "' class='" + bg +  "'>");
				document.write(eval(team1)[dArray[j]] + "</TD>");	
			}
		}
		document.write("</tr>");
	}
	return;
}


//	Grand Total Boxes
function BoxLeaderTable( title, columns, headname, dataname, totalname, xsize, iSortColumn, bDirection   )
{
	var h, i, j, k;		// make sure we set this as local
	var	TLA;
	var	bg = "TData1";				
	var TeamInfo;
	var url;
	var iMatch=0;
	var vparam;
	var rows=0;
	var iNext=0;
	var iCols =  columns / 2;
	var	number;

	
	dSort = new Array(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);
	
	
	
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
					t1 = eval(vparam)[i]-0;
					t2 =  eval(vparam)[dSort[j]]-0;
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





	for( h=0; h < iCols+1;)
	{
	
		document.write("<table align=center bgcolor=Silver>");
		document.write("<TR><TD valign=top  align=center>");
	
		document.write("<table class='TTitle' border=0 cellspacing=1 cellpadding=3 width='" + xsize + "'>");
		document.write("<tr>");
		document.write("<td align=center colspan=" + columns + " class='THead'>" + title);
		document.write("</td></tr>");
	

		// Draw Header row				
		document.write("<tr>");
		for(i=0; i < iCols; i++)
		{
			if(i==0)
			{
				document.write("<td align=left NoWrap  class='TSubHead2'>");
				document.write("<A class='TSubHead2' href='" + DisplaySortUrl( 0 ) + "'>");
				document.write("Team</A>" + "</td>");
			}
			else
			{
				document.write("<td align=center NoWrap  class='TSubHead2'>");
				document.write("<A class='TSubHead2' href='" + DisplaySortUrl( i+h ) + "'>");
				document.write(eval(headname)[i+h] + "</A></td>");
			}
		}
		document.write("</tr>");


		// Draw Data column by column
		document.write("<tr>");
		for(i=0; i < iCols; i++)
		{
			number = i + h;	
			vparam = dataname + number;
			
			rows = eval(vparam).length;
		
			if(bg == "TData2")		// change color of each column
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
					TLA = eval(vparam)[dSort[j]];
					url=  "team.html?team=" + TLA;
					document.write("<A class='team' href='" + url + "'>");
					document.write(eval(TLA)[1]  + "</A>");
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
		document.write("</TD></TR></TABLE>");
	
		h = h + iCols;
		document.write("<br>");
	}	// next table	
	
	
	
	
	
	return;
}


function DisplayBanner()
{
	document.write('<table width=600 cellspacing="0" cellpadding="0"  bgcolor="#000080" align=center>');
	document.write('<tr><td align="middle" valign="center">');
	document.write('<td align="middle" valign="center">');
	document.write('<IMG  alt="Strat-o-matic Logo" src="' + toplogo + '"  border=0 ></td>');
	document.write('<td align="middle" valign="center"><i><b><font face="Arial, Helvetica, sans-serif" color="#ffffff" size="4">'+ toptext +'<br></font></b></i></td>');
	document.write('</td></tr></table>');
	return;
}



function DisplayHeader()
{
	// Home
    document.write('<TABLE width=600 align=center bgColor=SteelBlue CELLSPACING="3" CELLPADDING="3" border="0"><TR>');

	document.write('<TD align="left">');
	document.write('<A href="index.html"><img border=0 src="' + mylogo + '" width=196 height=42 ></A></TD>');
	
	// League Leaders
	document.write("<TD align='middle'  class='MenuText1'>");
	document.write('<A href="leaders.html">League Leaders</A>');
	document.write('</TD>');

	// Grand Totals 
	document.write('<TD align="middle" class="MenuText1">');
	document.write('<A href="grand1.html">Team Batting Totals');
	document.write('<br><br>');

	document.write('<A href="grand2.html">Team Pitching Totals');
	document.write('</TD>');
		
		
	// Newspaper Style Recap
    document.write('<TD align="middle"  class="MenuText1"></TD>');

	document.write('</TR></TABLE>');

	return;
}

function BeginTable( backColor )
{
	document.write("<TABLE width=600 align=center bgcolor=" + backColor + ">");
	document.write("<TR><TD valign=top  align=center>");
}

function EndTable()
{
	document.write("</TD><TR></TABLE>");
}


function DisplayFooter()
{
	document.write('<table width=600 align=center border="0">');
	document.write('<tr bgcolor="#000080" valign="top"> ');
	document.write('<td align="middle" valign="center">');
	document.write('<font color="#ffffff" size="1" face="Arial, Helvetica, sans-serif">');
	document.write('© 2001-2002 Strat-O-Matic Game Co., Inc.- All rights reserved.</font>');
	document.write('</td></tr>');
	document.write('</table>');
	return;
}


function getParam(name)
{
var h = parent.location.href;
var nr = h.split("?");
var p;
var	s;
var params="";
var	vp;
var result="";
var i;

	if(( typeof(nr[1])== "undefined") )
	{
		params = "";
		return params;
	}	
	else
	{
		p=nr[1];
		params=p.split("&");
		for(i=0; i<params.length; i++)
		{
			s = params[i].split("=");		
			if(s[0] == name)
			{
				if( typeof(s[1])== "undefined") 
					result = "";
				else
					result = s[1];
				return result;
			}
		}
	}				
	return result;
}  

function cleanPlayerName( name )
{
	var temp;
	var temp1;
	
	temp = removeChars(name," ");
	temp1 = removeChars(temp,"'");
		
	return temp1;
}


function removeChars( rcname, rc )
{
	var	tx = rcname;
	var i;
	var ar = tx.split( rc );
	var temp2;
	
	temp2 = ar[0];
	for(i=1; i< ar.length; i++ )
	{
		temp2 = temp2 + ar[i];
	}	

	return temp2;
}
	
	

function DisplayPlayerURL( name, team, player )
{
	var url;
	var result;
	var cname;

	if(!name)
	{
		result = "<img src='pixel.gif' align='left' width=10 height=1 alt='Strat-O-Matic'>";
		return result;
	}
	
	var s = name.split(".");
	var vName;
	vName = s[0]+ " " + s[1];
	
	cname = cleanPlayerName(name);
	if( player == 0)
		url = "bplayer.html?" +  team + cname;	// batter
	else
		url = "pplayer.html?" +  team + cname ;	// pitcher

	result = "<A  class='player' href='" + url + "'>" + vName +  "</A>";
	return result;
	
}

function DisplayTeamURL( name )
{
	var temp;
	var url;
	var result;
	
	if(name == "***")
	{
		result = "***"
	}
	else
	{
		url = "team.html?team=" + name;
		result = "<A class='team' href='" + url + "'>" + name +  "</A>";
	}
	return result;
	
}

function DisplayBigTeam( name )
{
	var temp;
	var url;
	var result;
	
	url = "team.html?team=" + name;
	result = "<A class='PlayerName' href='" + url + "'>" + name +  "</A>";
	return result;
	
}
function DisplayCityURL( dname, name )
{
	var temp;
	var url;
	var result;
	
	url = "team.html?team=" + name;
	result = "<A class='PlayerName' href='" + url + "'>" + dname +  "</A>";
	return result;
	
}


function DisplaySortUrl(col)
{
var h = parent.location.href;
var nr = h.split("?");
var vParam = getParam("subleague");
var url;
var result;



	url = nr[0] + "?";
	if(vParam.length > 0 )
	{
		url = url + "subleague" + "=" + vParam + "&";
	}

	url = url + "sort=" + col;
	result = url;
	return result;
}

function TeamSortUrl(col,table)
{
var h = parent.location.href;
var nr = h.split("?");
var vParam = getParam("team");
var url;
var result;

	url = nr[0] + "?";
	if(vParam.length > 0 )
	{
		url = url + "team" + "=" + vParam + "&";
	}
	url = url + "sort=" + col;
	url = url + "&table=" + table;
	result = url;
	return result;
}


	
