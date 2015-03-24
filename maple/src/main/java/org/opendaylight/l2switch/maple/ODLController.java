/**
 * Copyright (c) 2013 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.l2switch.maple;

import org.maple.core.Controller;
import org.maple.core.MapleSystem;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.DataChangeListener;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.sal.binding.api.NotificationService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.FlowCapableNode;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.Table;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.Nodes;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.Node;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingService;
import org.opendaylight.yangtools.concepts.ListenerRegistration;
import org.opendaylight.yangtools.concepts.Registration;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listens to packetIn notification and 
 * <ul>
 * <li>in HUB mode simply floods all switch ports (except ingress port)</li>
 * <li>in LSWITCH mode collects source MAC address of packetIn and bind it with ingress port. 
 * If target MAC address is already bound then a flow is created (for direct communication between 
 * corresponding MACs)</li>
 * </ul>
 */

public class ODLController implements Controller,
                                      DataChangeListenerRegistrationHolder {
    
  protected static final Logger LOG = LoggerFactory
            .getLogger(ODLController.class);

  private NotificationService notificationService;
  private PacketProcessingService packetProcessingService;
  private DataBroker data;

  private Registration packetInRegistration;

/*
  private ListenerRegistration<DataChangeListener>
                                              dataChangeListenerRegistration;
*/

  private MapleSystem maple;

  /**
   * @param notificationService the notificationService to set
   */
  public void setNotificationService(NotificationService
                                                       notificationService) {
    this.notificationService = notificationService;
  }

  /**
   * @param packetProcessingService the packetProcessingService to set
   */
  public void setPacketProcessingService(PacketProcessingService
                                                   packetProcessingService) {
    this.packetProcessingService = packetProcessingService;
  }
    
  /**
   * @param data the data to set
   */
  public void setDataBroker(DataBroker data) {
    this.data = data;
  }

  /**
   * starting controller
   */
  public void start() {
    LOG.debug("start() -->");
    FlowCommitWrapper dataStoreAccessor = new FlowCommitWrapperImpl(data);

    this.maple = new MapleSystem(this);

    PacketHandler handler = new PacketHandler();
    handler.setRegistrationPublisher(this);
    handler.setDataStoreAccessor(dataStoreAccessor);
    handler.setPacketProcessingService(packetProcessingService);
    handler.setMapleSystem(maple);
    packetInRegistration = notificationService.registerNotificationListener(handler);

    WakeupOnNode wakeupListener = new WakeupOnNode();
    wakeupListener.setPacketHandler(handler);
/*
    dataChangeListenerRegistration = data.registerDataChangeListener(LogicalDatastoreType.OPERATIONAL,
            InstanceIdentifier.builder(Nodes.class)
                .child(Node.class)
                .augmentation(FlowCapableNode.class)
                .child(Table.class).build(),
            wakeupListener,
            DataBroker.DataChangeScope.SUBTREE);
*/
    LOG.debug("start() <--");
  }

  public void sendPacket(byte[] data, int... ports) {
    for (int i = 0; i < ports.length; i++) {
      System.out.println("ODLController sending packet to port " + ports[i]);
      // do something with ports[i]
    }
  }

  /**
   * stopping learning switch 
   */
  public void stop() {
    LOG.debug("stop() -->");
    //TODO: remove flow (created in #start())
    try {
      packetInRegistration.close();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    try {
      dataChangeListenerRegistration.close();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
      LOG.debug("stop() <--");
  }
    
  public ListenerRegistration<DataChangeListener> 
                                        getDataChangeListenerRegistration() {
    return dataChangeListenerRegistration;
  }
}
