

Ext.onReady(function(){
    Ext.History.init();

    var tree_panel = new Semantix.dv.TreePanel();

    var panel = new Ext.Panel({
        region: 'center',
        margins:'5 5 5 0',
        cmargins:'5 5 5 5',
        autoScroll: true,
        listeners: Semantix.dv.LinkInterceptor
    });

    Semantix.Bubbling.on('topics.selected', function(id) {
        updater = panel.getUpdater();

        updater.update({
            url: "/get_topic",
            params: {
                   entity_id: id
            }
        });

        Ext.History.add(id);
    });

    Ext.History.on('change', function(id){
        if(id) {
            Semantix.Bubbling.fire('topics.selected', id);
        }
    });

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

Semantix.dv.LinkInterceptor = {
    render: function(p){
        p.body.on({
            'mousedown': function(e, t){
                t.target = '_blank';
            },
            'click': function(e, t){
                e.stopEvent();
                Semantix.Bubbling.fire('topics.selected', t.id);
            },
            delegate:'a'
        });
    }
};
