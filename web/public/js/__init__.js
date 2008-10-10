Ext.BLANK_IMAGE_URL = '/public/ext/resources/images/default/s.gif';

Semantix = {}
Semantix.dv = {}

Semantix.msg = function(){
    var createBox = function(text) {
        return ['<div class="msg">',
                '<div class="x-box-tl"><div class="x-box-tr"><div class="x-box-tc"></div></div></div>',
                '<div class="x-box-ml"><div class="x-box-mr"><div class="x-box-mc">', text, '</div></div></div>',
                '<div class="x-box-bl"><div class="x-box-br"><div class="x-box-bc"></div></div></div>',
                '</div>'].join('');
    }

    if(!Semantix.hasOwnProperty('_msg_container')) {
        Semantix._msg_container = Ext.DomHelper.insertFirst(document.body, {id : 'semantix-msg-div'}, true);
    }

    Semantix._msg_container.alignTo(document, 't-t');

    Ext.DomHelper.append(
                            Semantix._msg_container,

                            { html : createBox(
                                                String.format.apply(String, arguments, 1)
                                              )
                            },

                            true
                        ).slideIn('t').pause(1).ghost("t", {remove:true});
}
