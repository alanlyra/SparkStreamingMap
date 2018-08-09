// <script src="../js/jquery/jquery-1.8.2.min.js"></script>
// <script src="../json-csv/base-jogadores.js"></script>
// <script type="text/javascript"> 

    paisClube = {};
    
    //Varre toda a base de dados de jogadores
    for(i=0;i<jogadores.length;i++){

        //Caso não exista, cria um array com a seleção do jogador
        if(!(jogadores[i]['Team'] in paisClube))
            paisClube[jogadores[i]['Team']] = {}
        
        //Caso não exista, cria um array dentro da seleção do jogador com o ano da copa que o jogador participou
        if(!(jogadores[i]['Year'] in paisClube[jogadores[i]['Team']]))
            paisClube[jogadores[i]['Team']][jogadores[i]['Year']] = {};
        
        //Caso não exista, cria um array dentro do ano da copa com o país do clube que o jogador jogava
        if(!(jogadores[i]['ClubCountry'] in paisClube[jogadores[i]['Team']][jogadores[i]['Year']]))
            paisClube[jogadores[i]['Team']][jogadores[i]['Year']][jogadores[i]['ClubCountry']] = 0;

        //Aumenta o número de jogadores do pais 'Team' no ano 'Year' que vieram de clubes do país 'ClubCountry'
        paisClube[jogadores[i]['Team']][jogadores[i]['Year']][jogadores[i]['ClubCountry']]++;
        
    }

    const ordered = {};
    Object.keys(paisClube).sort().forEach(function(key) {
      ordered[key] = paisClube[key];
    });

    paisClube = ordered;

// </script>
