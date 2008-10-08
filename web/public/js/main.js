

Ext.onReady(function(){

    var viewport = new Ext.Viewport({
        layout : 'border',
        items: [
            new DocsViewer.TreePanel(),
            new Ext.Panel({
                region: 'center',
                text : '123'
            })
        ]
    });

});
