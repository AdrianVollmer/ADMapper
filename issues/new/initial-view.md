When opening the application, first of all, do not show the demo data. Remove the demo data unless in `npm run dev`.

Next, when no connction string or an invalid conneciton string has been given
on the command line, show something where the graph usually is that indicates
that the user has to open a connection first. If there was an error parsing the
URL or creating the connection, show that error.
