import { storage } from '../storage';
import { importAllCSVData } from './csvImportService';
import bcrypt from 'bcryptjs';
import { db } from '../db';
import { users } from '@shared/schema';
import { eq } from 'drizzle-orm';

class MockDataService {
  async initializeBaseData() {
    console.log('Initializing base maritime data...');

    try {
      // Create admin user first
      await this.createAdminUser();
      
      // Create ports
      await this.createPorts();
      
      // Create vessels
      await this.createVessels();
      
      // Create storage sites
      await this.createStorageSites();
      
      // Generate initial statistics and data
      await this.generateInitialStats();
      
      // Create commodity pack data
      await this.createCrudeGrades();
      await this.createLngCargoes();
      await this.createDryBulkFixtures();
      await this.createPetrochemProducts();
      await this.createAgriBiofuelFlows();
      
      // Create new intelligence module data
      await this.createRefineries();
      await this.createSupplyDemandBalances();
      await this.createResearchReports();
      
      // Import CSV data for enhanced modules
      console.log('Importing CSV data for enhanced modules...');
      await importAllCSVData();
      
      // Final verification - log summary of seeded data
      const vesselCount = (await storage.getVessels()).length;
      const portCount = (await storage.getPorts()).length;
      console.log('=== INITIALIZATION COMPLETE ===');
      console.log(`Vessels in database: ${vesselCount}`);
      console.log(`Ports in database: ${portCount}`);
      console.log('Base maritime data initialized successfully');
    } catch (error) {
      console.error('Failed to initialize base data:', error);
      throw error; // Re-throw to surface errors
    }
  }

  private async createAdminUser() {
    try {
      // Check if admin user already exists
      const existingAdmin = await db.select().from(users).where(eq(users.email, 'admin@example.com')).limit(1);
      
      if (existingAdmin.length > 0) {
        console.log('Admin user already exists');
        return;
      }
      
      // Create admin user with bcrypt hashed password
      const passwordHash = await bcrypt.hash('admin123', 10);
      
      await db.insert(users).values({
        id: 'admin-user-id',
        email: 'admin@example.com',
        fullName: 'Admin User',
        name: 'Admin User',
        passwordHash,
        role: 'admin',
        isActive: true,
        createdAt: new Date(),
      });
      
      console.log('Created admin user: admin@example.com / admin123');
    } catch (error: any) {
      console.log('Admin user creation error:', error?.message || error);
    }
  }

  private async createPorts() {
    const ports = [
      {
        name: 'Fujairah',
        code: 'FJR',
        country: 'AE',
        latitude: '25.1204',
        longitude: '56.3541',
        timezone: 'Asia/Dubai',
        unlocode: 'AEFJR',
        countryCode: 'AE',
        geofenceRadiusKm: '10'
      },
      {
        name: 'Rotterdam',
        code: 'RTM',
        country: 'NL',
        latitude: '51.9225',
        longitude: '4.4792',
        timezone: 'Europe/Amsterdam',
        unlocode: 'NLRTM',
        countryCode: 'NL',
        geofenceRadiusKm: '15'
      },
      {
        name: 'Singapore',
        code: 'SIN',
        country: 'SG',
        latitude: '1.2644',
        longitude: '103.8200',
        timezone: 'Asia/Singapore',
        unlocode: 'SGSIN',
        countryCode: 'SG',
        geofenceRadiusKm: '12'
      }
    ];

    for (const port of ports) {
      try {
        await storage.createPort(port);
        console.log(`Created port: ${port.name}`);
      } catch (error: any) {
        // Port might already exist
        console.log(`Port ${port.name} already exists or error:`, error?.message || error);
      }
    }
  }

  private async createVessels() {
    const vessels = [
      {
        mmsi: '256148000',
        imo: '9387421',
        name: 'Seaways Pioneer',
        vesselType: 'vlcc',
        deadweight: 318000,
        flag: 'LR',
        operator: 'Seaways Shipping',
        buildYear: 2018
      },
      {
        mmsi: '235074166',
        imo: '9445231',
        name: 'Nordic Thunder',
        vesselType: 'suezmax',
        deadweight: 159000,
        flag: 'GB',
        operator: 'Nordic Maritime',
        buildYear: 2017
      },
      {
        mmsi: '636092932',
        imo: '9456123',
        name: 'Ocean Voyager',
        vesselType: 'aframax',
        deadweight: 115000,
        flag: 'LR',
        operator: 'Ocean Lines',
        buildYear: 2019
      },
      {
        mmsi: '538006575',
        imo: '9392847',
        name: 'Maritime Express',
        vesselType: 'vlcc',
        deadweight: 298000,
        flag: 'MH',
        operator: 'Express Maritime',
        buildYear: 2016
      },
      {
        mmsi: '477995700',
        imo: '9428394',
        name: 'Titan Carrier',
        vesselType: 'suezmax',
        deadweight: 164000,
        flag: 'HK',
        operator: 'Titan Shipping',
        buildYear: 2020
      },
      {
        mmsi: '244615000',
        imo: '9517293',
        name: 'Euro Trader',
        vesselType: 'aframax',
        deadweight: 109000,
        flag: 'NL',
        operator: 'Euro Marine',
        buildYear: 2018
      },
      {
        mmsi: '205544890',
        imo: '9445982',
        name: 'Baltic Pioneer',
        vesselType: 'suezmax',
        deadweight: 156000,
        flag: 'BE',
        operator: 'Baltic Shipping',
        buildYear: 2019
      },
      {
        mmsi: '563847291',
        imo: '9472639',
        name: 'Gulf Navigator',
        vesselType: 'aframax',
        deadweight: 112000,
        flag: 'AE',
        operator: 'Gulf Marine',
        buildYear: 2017
      }
    ];

    let created = 0;
    let existing = 0;
    for (const vessel of vessels) {
      try {
        await storage.createVessel(vessel);
        console.log(`Created vessel: ${vessel.name}`);
        created++;
      } catch (error: any) {
        console.log(`Vessel ${vessel.name} already exists or error:`, error?.message || error);
        existing++;
      }
    }
    
    // Verify vessels exist in database
    const allVessels = await storage.getVessels();
    console.log(`[VESSEL SEED] Created: ${created}, Already existed: ${existing}, Total in DB: ${allVessels.length}`);
    
    if (allVessels.length === 0) {
      console.error('[VESSEL SEED ERROR] No vessels found in database after seeding!');
    }
  }

  private async createStorageSites() {
    const ports = await storage.getPorts();
    
    for (const port of ports) {
      let sites: Array<{portId: string; name: string; siteType: string; capacity: number; latitude: string; longitude: string}> = [];
      
      if (port.code === 'FJR') {
        sites = [
          { portId: port.id, name: 'Fujairah Site A', siteType: 'tank_farm', capacity: 1500000, latitude: '25.1145', longitude: '56.3688' },
          { portId: port.id, name: 'Fujairah Site B', siteType: 'tank_farm', capacity: 2200000, latitude: '25.1267', longitude: '56.3633' },
          { portId: port.id, name: 'Fujairah Site C', siteType: 'tank_farm', capacity: 1800000, latitude: '25.1089', longitude: '56.3795' }
        ];
      } else if (port.code === 'RTM') {
        sites = [
          { portId: port.id, name: 'Rotterdam Tank Terminal A', siteType: 'tank_farm', capacity: 3500000, latitude: '51.9156', longitude: '4.4683' },
          { portId: port.id, name: 'Rotterdam Tank Terminal B', siteType: 'tank_farm', capacity: 4200000, latitude: '51.9298', longitude: '4.4521' }
        ];
      } else if (port.code === 'SIN') {
        sites = [
          { portId: port.id, name: 'Jurong Island Terminal', siteType: 'tank_farm', capacity: 5000000, latitude: '1.2650', longitude: '103.6800' },
          { portId: port.id, name: 'Pulau Bukom Terminal', siteType: 'tank_farm', capacity: 4500000, latitude: '1.2300', longitude: '103.7600' }
        ];
      }

      for (const site of sites) {
        try {
          const newSite = await storage.createStorageSite(site);
          console.log(`Created storage site: ${site.name}`);

          // Create initial fill data
          await storage.createStorageFillData({
            siteId: newSite.id,
            timestamp: new Date(),
            fillIndex: Math.random() * 0.6 + 0.2, // 20-80% fill
            confidence: 0.8 + Math.random() * 0.2,
            source: 'SAR',
            metadata: { initialization: true }
          });
        } catch (error: any) {
          console.log(`Storage site ${site.name} already exists or error:`, error?.message || error);
        }
      }
    }
  }

  private async generateInitialStats() {
    const ports = await storage.getPorts();
    
    for (const port of ports) {
      try {
        await storage.createPortStats({
          portId: port.id,
          date: new Date(),
          arrivals: Math.floor(Math.random() * 8) + 3,
          departures: Math.floor(Math.random() * 6) + 2,
          queueLength: Math.floor(Math.random() * 15) + 5,
          averageWaitHours: Math.random() * 10 + 8,
          totalVessels: Math.floor(Math.random() * 25) + 35,
          throughputMT: Math.random() * 1.5 + 1.2,
          byClass: {
            VLCC: Math.floor(Math.random() * 8) + 5,
            Suezmax: Math.floor(Math.random() * 12) + 8,
            Aframax: Math.floor(Math.random() * 20) + 15
          }
        });
        console.log(`Created initial statistics for ${port.name}`);
      } catch (error: any) {
        console.log(`Stats for ${port.name} already exist or error:`, error?.message || error);
      }
    }
  }

  private async createCrudeGrades() {
    const crudeGrades = [
      {
        name: 'Brent Crude',
        gradeCode: 'BRENT',
        category: 'light',
        origin: 'North Sea',
        apiGravity: '38.3',
        sulfurContent: '0.37',
        viscosity: '3.5',
        pourPoint: '-9',
        yieldProfile: { distillate: 45, gasoline: 22, residual: 18, naphtha: 15 },
        priceBenchmark: 'Brent',
        currentPrice: '82.50',
        priceUnit: 'USD/bbl',
        specifications: { tan: 0.05, metals: 'low' }
      },
      {
        name: 'WTI Crude',
        gradeCode: 'WTI',
        category: 'light',
        origin: 'USA',
        apiGravity: '39.6',
        sulfurContent: '0.24',
        viscosity: '3.2',
        pourPoint: '-12',
        yieldProfile: { distillate: 48, gasoline: 24, residual: 15, naphtha: 13 },
        priceBenchmark: 'WTI',
        currentPrice: '79.80',
        priceUnit: 'USD/bbl',
        specifications: { tan: 0.04, metals: 'very_low' }
      },
      {
        name: 'Arab Light',
        gradeCode: 'ARBL',
        category: 'medium',
        origin: 'Saudi Arabia',
        apiGravity: '33.4',
        sulfurContent: '1.77',
        viscosity: '6.8',
        pourPoint: '-6',
        yieldProfile: { distillate: 38, gasoline: 18, residual: 28, naphtha: 16 },
        priceBenchmark: 'Dubai',
        currentPrice: '76.20',
        priceUnit: 'USD/bbl',
        specifications: { tan: 0.15, metals: 'moderate' }
      },
      {
        name: 'Urals Crude',
        gradeCode: 'URALS',
        category: 'medium',
        origin: 'Russia',
        apiGravity: '31.7',
        sulfurContent: '1.35',
        viscosity: '8.2',
        pourPoint: '-18',
        yieldProfile: { distillate: 36, gasoline: 16, residual: 32, naphtha: 16 },
        priceBenchmark: 'Brent',
        currentPrice: '72.40',
        priceUnit: 'USD/bbl',
        specifications: { tan: 0.18, metals: 'moderate' }
      },
      {
        name: 'Maya Crude',
        gradeCode: 'MAYA',
        category: 'heavy',
        origin: 'Mexico',
        apiGravity: '22.0',
        sulfurContent: '3.30',
        viscosity: '95.0',
        pourPoint: '15',
        yieldProfile: { distillate: 28, gasoline: 12, residual: 45, naphtha: 15 },
        priceBenchmark: 'WTI',
        currentPrice: '65.30',
        priceUnit: 'USD/bbl',
        specifications: { tan: 0.45, metals: 'high' }
      },
      {
        name: 'Dubai Crude',
        gradeCode: 'DUBAI',
        category: 'medium',
        origin: 'UAE',
        apiGravity: '31.0',
        sulfurContent: '2.04',
        viscosity: '7.5',
        pourPoint: '-3',
        yieldProfile: { distillate: 35, gasoline: 17, residual: 30, naphtha: 18 },
        priceBenchmark: 'Dubai',
        currentPrice: '77.90',
        priceUnit: 'USD/bbl',
        specifications: { tan: 0.20, metals: 'moderate' }
      }
    ];

    for (const grade of crudeGrades) {
      try {
        await storage.createCrudeGrade(grade);
        console.log(`Created crude grade: ${grade.name}`);
      } catch (error: any) {
        console.log(`Crude grade ${grade.name} already exists or error:`, error?.message || error);
      }
    }
  }

  private async createLngCargoes() {
    const ports = await storage.getPorts();
    const vessels = await storage.getVessels();
    
    const lngCargoes = [
      {
        cargoId: 'LNG-2024-001',
        cargoType: 'LNG',
        vesselId: vessels[0]?.id || null,
        loadPortId: ports[0]?.id || null,
        dischargePortId: ports[1]?.id || null,
        volume: '145000',
        volumeUnit: 'm3',
        isDiversion: false,
        loadDate: new Date('2024-10-01'),
        dischargeDate: new Date('2024-10-15'),
        price: '18.50',
        priceUnit: 'USD/mmbtu',
        buyer: 'Global Gas Ltd',
        seller: 'Energy Corp',
        contractType: 'spot'
      },
      {
        cargoId: 'LNG-2024-002',
        cargoType: 'LNG',
        vesselId: vessels[1]?.id || null,
        loadPortId: ports[0]?.id || null,
        dischargePortId: ports[1]?.id || null,
        volume: '138000',
        volumeUnit: 'm3',
        isDiversion: true,
        originalDestination: 'Asia Pacific',
        loadDate: new Date('2024-10-05'),
        price: '19.20',
        priceUnit: 'USD/mmbtu',
        buyer: 'Pacific Energy',
        seller: 'LNG Suppliers Inc',
        contractType: 'fob'
      },
      {
        cargoId: 'LPG-2024-001',
        cargoType: 'LPG',
        vesselId: vessels[2]?.id || null,
        loadPortId: ports[1]?.id || null,
        dischargePortId: ports[0]?.id || null,
        volume: '44000',
        volumeUnit: 'MT',
        isDiversion: false,
        loadDate: new Date('2024-10-08'),
        dischargeDate: new Date('2024-10-20'),
        price: '650',
        priceUnit: 'USD/MT',
        buyer: 'Petro Logistics',
        seller: 'Global Fuels',
        contractType: 'term'
      }
    ];

    for (const cargo of lngCargoes) {
      try {
        await storage.createLngCargo(cargo);
        console.log(`Created LNG cargo: ${cargo.cargoId}`);
      } catch (error: any) {
        console.log(`LNG cargo ${cargo.cargoId} already exists or error:`, error?.message || error);
      }
    }
  }

  private async createDryBulkFixtures() {
    const ports = await storage.getPorts();
    
    const fixtures = [
      {
        fixtureId: 'DBF-2024-001',
        commodityType: 'Coal',
        quantity: '175000',
        loadPortId: ports[0]?.id || null,
        dischargePortId: ports[1]?.id || null,
        vesselSize: 'Capesize',
        freightRate: '18500',
        laycanStart: new Date('2024-10-15'),
        laycanEnd: new Date('2024-10-20'),
        fixtureDate: new Date('2024-10-12'),
        charterer: 'Global Mining Co'
      },
      {
        fixtureId: 'DBF-2024-002',
        commodityType: 'Iron Ore',
        quantity: '180000',
        loadPortId: ports[1]?.id || null,
        dischargePortId: ports[0]?.id || null,
        vesselSize: 'Capesize',
        freightRate: '19200',
        laycanStart: new Date('2024-10-18'),
        laycanEnd: new Date('2024-10-23'),
        fixtureDate: new Date('2024-10-14'),
        charterer: 'Steel Mills Inc'
      },
      {
        fixtureId: 'DBF-2024-003',
        commodityType: 'Grain',
        quantity: '55000',
        loadPortId: ports[0]?.id || null,
        dischargePortId: ports[1]?.id || null,
        vesselSize: 'Panamax',
        freightRate: '12800',
        laycanStart: new Date('2024-10-22'),
        laycanEnd: new Date('2024-10-27'),
        fixtureDate: new Date('2024-10-16'),
        charterer: 'Agri Trade Ltd'
      }
    ];

    for (const fixture of fixtures) {
      try {
        await storage.createDryBulkFixture(fixture);
        console.log(`Created dry bulk fixture: ${fixture.fixtureId}`);
      } catch (error: any) {
        console.log(`Dry bulk fixture ${fixture.fixtureId} already exists or error:`, error?.message || error);
      }
    }
  }

  private async createPetrochemProducts() {
    const products = [
      {
        productCode: 'PC-ETH-001',
        productName: 'Ethylene',
        category: 'Olefins',
        feedstock: 'Naphtha',
        yieldRate: '32.5',
        currentPrice: '1250.00',
        marginSpread: '185.50',
        utilizationRate: '87.3',
        region: 'Asia Pacific'
      },
      {
        productCode: 'PC-BEN-001',
        productName: 'Benzene',
        category: 'Aromatics',
        feedstock: 'Crude Oil',
        yieldRate: '18.2',
        currentPrice: '980.00',
        marginSpread: '145.20',
        utilizationRate: '92.1',
        region: 'Europe'
      },
      {
        productCode: 'PC-PE-001',
        productName: 'Polyethylene',
        category: 'Polymers',
        feedstock: 'Ethylene',
        yieldRate: '95.5',
        currentPrice: '1450.00',
        marginSpread: '225.80',
        utilizationRate: '89.7',
        region: 'North America'
      },
      {
        productCode: 'PC-MEG-001',
        productName: 'Monoethylene Glycol',
        category: 'Intermediates',
        feedstock: 'Ethylene Oxide',
        yieldRate: '78.3',
        currentPrice: '820.00',
        marginSpread: '112.40',
        utilizationRate: '85.6',
        region: 'Middle East'
      }
    ];

    for (const product of products) {
      try {
        await storage.createPetrochemProduct(product);
        console.log(`Created petrochem product: ${product.productCode}`);
      } catch (error: any) {
        console.log(`Petrochem product ${product.productCode} already exists or error:`, error?.message || error);
      }
    }
  }

  private async createAgriBiofuelFlows() {
    const flows = [
      {
        flowId: 'AB-2024-001',
        commodityType: 'Soybean Oil',
        flowType: 'export',
        originCountry: 'Brazil',
        destinationCountry: 'China',
        volume: '85000',
        volumeUnit: 'MT',
        biofuelType: 'biodiesel',
        feedstock: 'soybean',
        sustainabilityCert: 'ISCC',
        carbonIntensity: '32.5',
        price: '1450.00',
        priceUnit: 'USD/ton',
        flowDate: new Date('2024-10-12'),
        trader: 'Global Agri Corp'
      },
      {
        flowId: 'AB-2024-002',
        commodityType: 'Palm Oil',
        flowType: 'export',
        originCountry: 'Malaysia',
        destinationCountry: 'Netherlands',
        volume: '62000',
        volumeUnit: 'MT',
        biofuelType: 'HVO',
        feedstock: 'palm_oil',
        sustainabilityCert: 'RSPO',
        carbonIntensity: '28.8',
        price: '1380.00',
        priceUnit: 'USD/ton',
        flowDate: new Date('2024-10-10'),
        trader: 'Palm Logistics Ltd'
      },
      {
        flowId: 'AB-2024-003',
        commodityType: 'Corn Ethanol',
        flowType: 'production',
        originCountry: 'United States',
        destinationCountry: 'United States',
        volume: '125000',
        volumeUnit: 'liters',
        biofuelType: 'ethanol',
        feedstock: 'corn',
        sustainabilityCert: null,
        carbonIntensity: '45.2',
        price: '0.65',
        priceUnit: 'USD/liter',
        flowDate: new Date('2024-10-08'),
        trader: 'Midwest Biofuels'
      }
    ];

    for (const flow of flows) {
      try {
        await storage.createAgriBiofuelFlow(flow);
        console.log(`Created agri flow: ${flow.flowId}`);
      } catch (error: any) {
        console.log(`Agri flow ${flow.flowId} already exists or error:`, error?.message || error);
      }
    }
  }

  private async createRefineries() {
    const refineries = [
      {
        name: 'Houston Refinery Complex',
        refineryCode: 'REF-HOU-001',
        country: 'United States',
        region: 'North America',
        operator: 'Marathon Petroleum',
        capacity: '585000',
        currentThroughput: '540000',
        utilizationRate: '92.31',
        complexityIndex: '11.8',
        yieldGasoline: '45.2',
        yieldDiesel: '28.5',
        yieldJetFuel: '12.3',
        yieldOther: '14.0',
        maintenanceStatus: 'operational',
        marginPerBarrel: '18.50'
      },
      {
        name: 'Singapore Refinery',
        refineryCode: 'REF-SIN-001',
        country: 'Singapore',
        region: 'Asia Pacific',
        operator: 'Shell Eastern',
        capacity: '500000',
        currentThroughput: '475000',
        utilizationRate: '95.00',
        complexityIndex: '9.5',
        yieldGasoline: '38.0',
        yieldDiesel: '32.0',
        yieldJetFuel: '18.0',
        yieldOther: '12.0',
        maintenanceStatus: 'operational',
        marginPerBarrel: '16.25'
      },
      {
        name: 'Rotterdam Refinery',
        refineryCode: 'REF-RTM-001',
        country: 'Netherlands',
        region: 'Europe',
        operator: 'BP',
        capacity: '400000',
        currentThroughput: '360000',
        utilizationRate: '90.00',
        complexityIndex: '10.2',
        yieldGasoline: '42.0',
        yieldDiesel: '30.0',
        yieldJetFuel: '15.0',
        yieldOther: '13.0',
        maintenanceStatus: 'planned_maintenance',
        maintenanceStart: new Date('2024-11-01'),
        maintenanceEnd: new Date('2024-11-15'),
        marginPerBarrel: '14.75'
      },
      {
        name: 'Saudi Aramco Ras Tanura',
        refineryCode: 'REF-RST-001',
        country: 'Saudi Arabia',
        region: 'Middle East',
        operator: 'Saudi Aramco',
        capacity: '550000',
        currentThroughput: '520000',
        utilizationRate: '94.55',
        complexityIndex: '8.7',
        yieldGasoline: '35.0',
        yieldDiesel: '35.0',
        yieldJetFuel: '20.0',
        yieldOther: '10.0',
        maintenanceStatus: 'operational',
        marginPerBarrel: '22.00'
      }
    ];

    for (const refinery of refineries) {
      try {
        await storage.createRefinery(refinery);
        console.log(`Created refinery: ${refinery.name}`);
      } catch (error: any) {
        console.log(`Refinery ${refinery.refineryCode} already exists or error:`, error?.message || error);
      }
    }
  }

  private async createSupplyDemandBalances() {
    const balances = [
      {
        balanceId: 'BAL-2024-Q3-CRUDE-GLOBAL',
        commodity: 'crude_oil',
        region: 'global',
        period: '2024_Q3',
        production: '102500',
        consumption: '101800',
        imports: '45000',
        exports: '45000',
        inventoryChange: '700',
        closingInventory: '2950',
        balanceValue: '700',
        unit: 'kbd',
        forecastType: 'actual',
        dataSource: 'IEA'
      },
      {
        balanceId: 'BAL-2024-Q4-CRUDE-GLOBAL',
        commodity: 'crude_oil',
        region: 'global',
        period: '2024_Q4',
        production: '103200',
        consumption: '102500',
        imports: '46000',
        exports: '46000',
        inventoryChange: '700',
        closingInventory: '3650',
        balanceValue: '700',
        unit: 'kbd',
        forecastType: 'forecast',
        dataSource: 'IEA'
      },
      {
        balanceId: 'BAL-2024-Q3-CRUDE-ASIA',
        commodity: 'crude_oil',
        region: 'asia',
        period: '2024_Q3',
        production: '8500',
        consumption: '35000',
        imports: '28000',
        exports: '1500',
        inventoryChange: '-500',
        closingInventory: '1200',
        balanceValue: '-500',
        unit: 'kbd',
        forecastType: 'actual',
        dataSource: 'IEA'
      },
      {
        balanceId: 'BAL-2024-Q3-LNG-GLOBAL',
        commodity: 'lng',
        region: 'global',
        period: '2024_Q3',
        production: '400',
        consumption: '395',
        imports: '350',
        exports: '350',
        inventoryChange: '5',
        closingInventory: '45',
        balanceValue: '5',
        unit: 'MT',
        forecastType: 'actual',
        dataSource: 'GIIGNL'
      },
      {
        balanceId: 'BAL-2024-Q3-GASOLINE-NAMERICA',
        commodity: 'gasoline',
        region: 'north_america',
        period: '2024_Q3',
        production: '9800',
        consumption: '9500',
        imports: '500',
        exports: '800',
        inventoryChange: '300',
        closingInventory: '220',
        balanceValue: '300',
        unit: 'kbd',
        forecastType: 'actual',
        dataSource: 'EIA'
      }
    ];

    for (const balance of balances) {
      try {
        await storage.createSupplyDemandBalance(balance);
        console.log(`Created balance: ${balance.balanceId}`);
      } catch (error: any) {
        console.log(`Balance ${balance.balanceId} already exists or error:`, error?.message || error);
      }
    }
  }

  private async createResearchReports() {
    const reports = [
      {
        reportId: 'RPT-2024-OCT-001',
        title: 'Q4 2024 Crude Oil Market Outlook: OPEC+ Cuts and Asian Demand',
        category: 'market_analysis',
        subcategory: 'crude_oil',
        summary: 'Comprehensive analysis of global crude oil markets for Q4 2024, examining OPEC+ production policies, Asian demand recovery, and inventory dynamics. The report forecasts moderate price upside supported by supply discipline and improving Chinese economic indicators.',
        keyInsights: JSON.stringify([
          'OPEC+ extending voluntary cuts through Q4 2024',
          'Chinese demand showing signs of recovery with +2.5% growth',
          'US shale production plateauing around 13.2 million bpd',
          'Global inventories declining by 0.7 million bpd',
          'Geopolitical premium adding $5-8/bbl to Brent'
        ]),
        priceOutlook: 'bullish',
        shortTermForecast: 'Brent crude expected to trade in $85-95/bbl range through Q4 2024, supported by OPEC+ discipline and seasonal demand.',
        mediumTermForecast: 'Prices likely to moderate to $80-90/bbl in H1 2025 as US production growth resumes and OPEC+ begins unwinding cuts.',
        longTermForecast: 'Structural shift toward $70-85/bbl equilibrium by 2026-2027 as energy transition accelerates and non-OPEC supply increases.',
        analyst: 'Sarah Chen, Chief Energy Analyst',
        confidenceLevel: 'high',
        dataPoints: JSON.stringify({
          brentForecast: { q4_2024: 90, q1_2025: 85, q2_2025: 82 },
          demandGrowth: { 2024: 1.2, 2025: 1.5 },
          inventoryCover: { current: 32, historical_avg: 35 }
        }),
        tags: ['crude_oil', 'opec', 'china', 'price_forecast', 'supply_demand'],
        publishDate: new Date('2024-10-01'),
        isPublished: true
      },
      {
        reportId: 'RPT-2024-OCT-002',
        title: 'LNG Market Dynamics: European Storage and Asian Spot Prices',
        category: 'price_forecast',
        subcategory: 'lng',
        summary: 'Analysis of LNG market fundamentals highlighting European storage levels at 95% capacity, weakening Asian spot prices, and implications for winter 2024-25 trade flows.',
        keyInsights: JSON.stringify([
          'European storage at 95%, reducing urgent import needs',
          'Asian spot JKM prices down 40% YoY to $12-13/MMBtu',
          'New US export capacity adding 25 MTPA in 2024-2025',
          'Increased floating storage activity signaling oversupply',
          'Long-term contract renegotiations favoring buyers'
        ]),
        priceOutlook: 'bearish',
        shortTermForecast: 'Asian spot prices expected to remain weak at $11-14/MMBtu through winter 2024-25 due to oversupply and high European stocks.',
        mediumTermForecast: 'Gradual price recovery to $15-18/MMBtu in 2025 as new demand from Bangladesh and Vietnam ramps up.',
        longTermForecast: 'Structural oversupply likely to cap prices at $18-22/MMBtu through 2027 as new projects (Qatar, US) come online.',
        analyst: 'Michael Rodriguez, LNG Markets',
        confidenceLevel: 'medium',
        dataPoints: JSON.stringify({
          jkmForecast: { winter_24: 13, summer_25: 11, winter_25: 16 },
          supplyGrowth: { 2024: 25, 2025: 35, 2026: 40 },
          europeanStorage: { current: 95, target: 90 }
        }),
        tags: ['lng', 'europe', 'asia', 'spot_prices', 'storage'],
        publishDate: new Date('2024-10-05'),
        isPublished: true
      },
      {
        reportId: 'RPT-2024-OCT-003',
        title: 'Refining Margins Under Pressure: Diesel Cracks and Capacity Additions',
        category: 'market_analysis',
        subcategory: 'refining',
        summary: 'Deep dive into global refining margins analyzing weakening diesel cracks, new capacity startups in Asia and Middle East, and implications for refinery economics and run rates.',
        keyInsights: JSON.stringify([
          'Diesel cracks declining to $25/bbl from $40/bbl peak',
          'New refining capacity of 2.5 million bpd in Asia/ME in 2024',
          'European refinery utilization down to 82% from 88%',
          'Complex refinery margins holding better than simple refineries',
          'IMO 2020 diesel premium normalizing'
        ]),
        priceOutlook: 'neutral',
        shortTermForecast: 'Refining margins expected to remain under pressure through Q4 2024 at $12-15/bbl for complex refineries as new capacity ramps up.',
        mediumTermForecast: 'Gradual margin recovery to $16-18/bbl in 2025 as older, less efficient capacity is rationalized.',
        longTermForecast: 'Structural margin compression to $14-17/bbl by 2026-2027 as demand growth slows and overcapacity persists.',
        analyst: 'David Thompson, Refining & Products',
        confidenceLevel: 'medium',
        dataPoints: JSON.stringify({
          crackSpreads: { gasoline: 18, diesel: 25, jetFuel: 22 },
          capacityAdditions: { 2024: 2.5, 2025: 1.8, 2026: 1.2 },
          utilizationRates: { north_america: 89, europe: 82, asia: 86 }
        }),
        tags: ['refining', 'margins', 'diesel', 'capacity', 'asia'],
        publishDate: new Date('2024-10-08'),
        isPublished: true
      }
    ];

    for (const report of reports) {
      try {
        await storage.createResearchReport(report);
        console.log(`Created research report: ${report.reportId}`);
      } catch (error: any) {
        console.log(`Report ${report.reportId} already exists or error:`, error?.message || error);
      }
    }
  }
}

export const mockDataService = new MockDataService();
