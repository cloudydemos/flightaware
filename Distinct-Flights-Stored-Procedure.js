    function getDistinctFlights() {
        var context = getContext();
        var container = context.getCollection();
        var response = context.getResponse();

        // query for players
        var filterQuery =
        {
            'query' : 'SELECT distinct(c.flight) FROM c where c.Timestamp = 1634905178.5 group by c.flight, c.Timestamp'    
        };

        var flights = container.queryDocuments(container.getSelfLink(), filterQuery, {},
            function (err, items, responseOptions) {
                if (err) throw new Error("Error" + err.message);

                if (items.length = 0) throw "No flights?!?";

                getContext().getResponse().setBody(items);
            });
    }