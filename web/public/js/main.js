

Ext.onReady(function(){
    var tree_panel = new DocsViewer.TreePanel();

    var panel = new Ext.Panel({
        region: 'center',
        margins:'5 5 5 0',
        cmargins:'5 5 5 5',
        autoScroll: true,
        listeners: DocsViewer.LinkInterceptor
    });

    DocsViewer.update = function(id) {
        updater = panel.getUpdater();

        updater.update({
            url: "/get_topic",
            params: {
                   entity_id: id
            }
        });
    };

    tree_panel.on('topicselect', function(entity_id) {
        DocsViewer.update(entity_id);
    }, this);

    var viewport = new Ext.Viewport({
        layout : 'border',
        items: [
            tree_panel,
            panel
        ]
    });

});

DocsViewer.LinkInterceptor = {
    render: function(p){
        p.body.on({
            'mousedown': function(e, t){
                t.target = '_blank';
            },
            'click': function(e, t){
                e.stopEvent();
                DocsViewer.update(t.id);
            },
            delegate:'a'
        });
    }
};
