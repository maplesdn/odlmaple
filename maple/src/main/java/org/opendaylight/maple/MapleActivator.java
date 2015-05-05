/**
 * Copyright (c) 2013 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.maple;

import org.opendaylight.controller.sal.binding.api.AbstractBindingAwareConsumer;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.ConsumerContext;

import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.service.rev130819.SalFlowService;

import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingService;
import org.opendaylight.yangtools.concepts.ListenerRegistration;
import org.opendaylight.yangtools.yang.binding.NotificationListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingListener;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.common.api.data.AsyncDataBroker.DataChangeScope;
import org.opendaylight.controller.md.sal.binding.api.DataChangeListener;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.Nodes;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.Node;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.FlowCapableNode;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.Table;

import org.opendaylight.controller.sal.binding.api.NotificationService;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.NetworkTopology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.TopologyId;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.Topology;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.TopologyKey;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.Link;

/**
 * Maple activator.
 * 
 * Activator is derived from AbstractBindingAwareConsumer, which takes care
 * of looking up MD-SAL in Service Registry and registering consumer
 * when MD-SAL is present.
 */
public class MapleActivator extends AbstractBindingAwareConsumer implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(MapleActivator.class);

  private ODLController controller;

  /* Listener for packet-in event. */
  private ListenerRegistration<NotificationListener> notificationListenerReg;
  /* Listener for data-change events. */
  private ListenerRegistration<DataChangeListener> dataChangeListenerReg;

  private static final String TOPOLOGY_NAME = "flow:1";

  @Override
  protected void startImpl(BundleContext context) {
    LOG.info("startImpl() passing");
  }

  /**
   * Invoked when consumer is registered to the MD-SAL.
   */
  @Override
  public void onSessionInitialized(ConsumerContext session) {
    LOG.info("inSessionInitialized() passing");

    PacketProcessingService pps = session.getRpcService(PacketProcessingService.class);
    SalFlowService fs = session.getRpcService(SalFlowService.class);
    this.controller = new ODLController(pps, fs);

    DataBroker db = session.getSALService(DataBroker.class);
    this.controller.setDataStoreAccessor(new FlowCommitWrapperImpl(db));

    NotificationService ns = session.getSALService(NotificationService.class);
    this.notificationListenerReg = ns.registerNotificationListener(this.controller);

    /* Add listener for switch changes. */
    this.dataChangeListenerReg = db.registerDataChangeListener(
      LogicalDatastoreType.OPERATIONAL,
      InstanceIdentifier.builder(Nodes.class)
        .child(Node.class)
        .augmentation(FlowCapableNode.class)
        .child(Table.class).build(),
      this.controller,
      DataChangeScope.SUBTREE);

    /* Add listener for link changes. */
    this.dataChangeListenerReg = db.registerDataChangeListener(
      LogicalDatastoreType.OPERATIONAL,
      InstanceIdentifier.builder(NetworkTopology.class)
        .child(Topology.class, new TopologyKey(new TopologyId(TOPOLOGY_NAME)))
        .child(Link.class).build(),
      this.controller,
      DataChangeScope.BASE);

    this.controller.start();
  }

  @Override
  public void close() {
    LOG.info("close() passing");

    try {
      this.dataChangeListenerReg.close();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
/*
    try {
      this.notificationListenerReg.close();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
*/
    if (this.controller != null) {
      this.controller.stop();
    }
  }

  @Override
  protected void stopImpl(BundleContext context) {
    close();
    super.stopImpl(context);
  }
}
