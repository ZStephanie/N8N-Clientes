create schema if not exists ar;
create extension if not exists pgcrypto;

-- 1) Tipos enumerados 
do $$ begin
  if to_regtype('ar.tipo_persona') is null then
    create type ar.tipo_persona as enum ('NATURAL','JURIDICA');
  end if;
end $$;

do $$ begin
  if to_regtype('ar.estado_cliente') is null then
    create type ar.estado_cliente as enum ('ACTIVO','INACTIVO');
  end if;
end $$;

do $$ begin
  if to_regtype('ar.estado_factura') is null then
    create type ar.estado_factura as enum ('BORRADOR','EMITIDA','VENCIDA','PARCIAL','PAGADA','ANULADA');
  end if;
end $$;

do $$ begin
  if to_regtype('ar.moneda') is null then
    create type ar.moneda as enum ('COP','USD','EUR');
  end if;
end $$;

do $$ begin
  if to_regtype('ar.metodo_pago') is null then
    create type ar.metodo_pago as enum ('EFECTIVO','TRANSFERENCIA','TARJETA','CHEQUE','PSE','OTRO');
  end if;
end $$;

do $$ begin
  if to_regtype('ar.tipo_gestion') is null then
    create type ar.tipo_gestion as enum ('LLAMADA','CORREO','VISITA','RECORDATORIO','WHATSAPP','ACUERDO_PAGO','OTRO');
  end if;
end $$;

do $$ begin
  if to_regtype('ar.resultado_gestion') is null then
    create type ar.resultado_gestion as enum ('CONTACTADO','NO_CONTACTADO','COMPROMISO','INCUMPLIMIENTO','PAGADO','OTRO');
  end if;
end $$;

-- 2.1) Tenant actual desde el JWT (claim tenant_id)
create or replace function ar.current_tenant()
returns uuid
language sql stable as $$
  select nullif(current_setting('request.jwt.claims', true)::jsonb->>'tenant_id','')::uuid;
$$;

-- 2.2) Timestamp de actualización
create or replace function ar.set_updated_at()
returns trigger language plpgsql as $$
begin
  new.updated_at := now();
  return new;
end; $$;

-- 2.3) Validación de asignaciones de pago (consistencia / límites)
create or replace function ar.trg_validate_allocation()
returns trigger language plpgsql as $$
declare
  v_moneda_pago ar.moneda;
  v_moneda_fact ar.moneda;
  v_tenant_pago uuid;
  v_tenant_fact uuid;
  v_total numeric(14,2);
  v_aplicado numeric(14,2);
  v_saldo numeric(14,2);
begin
  select moneda, tenant_id into v_moneda_pago, v_tenant_pago
  from ar.pagos where id = new.pago_id;

  select moneda, tenant_id, total into v_moneda_fact, v_tenant_fact, v_total
  from ar.facturas where id = new.factura_id;

  if v_tenant_pago is distinct from v_tenant_fact then
    raise exception 'Pago y Factura pertenecen a distintos tenants';
  end if;
  if v_moneda_pago is distinct from v_moneda_fact then
    raise exception 'Moneda de pago (%) difiere de la factura (%)', v_moneda_pago, v_moneda_fact;
  end if;

  -- saldo actual de la factura = total - sum(asignaciones existentes excepto esta)
  select coalesce(sum(monto_aplicado),0) into v_aplicado
  from ar.pago_aplicaciones
  where factura_id = new.factura_id
    and id <> coalesce(new.id, '00000000-0000-0000-0000-000000000000');

  v_saldo := v_total - v_aplicado;
  if new.monto_aplicado > v_saldo then
    raise exception 'El monto aplicado (%) excede el saldo disponible (%) de la factura', new.monto_aplicado, v_saldo;
  end if;

  return new;
end; $$;

-- 3) Tablas (3NF) -------------------------------------------
-- 3.1) Clientes
create table if not exists ar.clientes (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null default ar.current_tenant(),
  tipo_persona ar.tipo_persona not null,
  nit text not null,
  dv smallint,
  razon_social text not null,
  nombre_comercial text,
  email text,
  telefono text,
  direccion text,
  ciudad text,
  departamento text,
  pais text,
  plazo_dias integer not null default 30 check (plazo_dias >= 0),
  cupo_credito numeric(14,2) not null default 0 check (cupo_credito >= 0),
  estado ar.estado_cliente not null default 'ACTIVO',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint uq_clientes_tenant_nit unique (tenant_id, nit)
);
drop trigger if exists tg_clientes_updated on ar.clientes;
create trigger tg_clientes_updated
before update on ar.clientes for each row execute function ar.set_updated_at();
create index if not exists ix_clientes_tenant on ar.clientes(tenant_id);

-- 3.2) Facturas
create table if not exists ar.facturas (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null default ar.current_tenant(),
  cliente_id uuid not null references ar.clientes(id) on delete restrict,
  serie text not null default 'A',
  numero text not null,
  secuencia integer,
  fecha_emision date not null default current_date,
  fecha_vencimiento date not null,
  moneda ar.moneda not null default 'COP',
  subtotal numeric(14,2) not null check (subtotal >= 0),
  impuestos numeric(14,2) not null default 0 check (impuestos >= 0),
  total numeric(14,2) generated always as (subtotal + impuestos) stored,
  estado ar.estado_factura not null default 'EMITIDA',
  notas text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint uq_factura unique (tenant_id, serie, numero),
  constraint chk_fechas check (fecha_vencimiento >= fecha_emision)
);
drop trigger if exists tg_facturas_updated on ar.facturas;
create trigger tg_facturas_updated
before update on ar.facturas for each row execute function ar.set_updated_at();
create index if not exists ix_facturas_cliente on ar.facturas(cliente_id);
create index if not exists ix_facturas_tenant_estado on ar.facturas(tenant_id, estado);

-- 3.3) Pagos (encabezado)
create table if not exists ar.pagos (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null default ar.current_tenant(),
  cliente_id uuid not null references ar.clientes(id) on delete restrict,
  fecha date not null default current_date,
  metodo ar.metodo_pago not null,
  referencia text,
  moneda ar.moneda not null default 'COP',
  monto_total numeric(14,2) not null check (monto_total > 0),
  notas text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
drop trigger if exists tg_pagos_updated on ar.pagos;
create trigger tg_pagos_updated
before update on ar.pagos for each row execute function ar.set_updated_at();
create index if not exists ix_pagos_cliente on ar.pagos(cliente_id);

-- 3.4) Aplicaciones de pago (detalle N..M pagos<->facturas)
create table if not exists ar.pago_aplicaciones (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null default ar.current_tenant(),
  pago_id uuid not null references ar.pagos(id) on delete cascade,
  factura_id uuid not null references ar.facturas(id) on delete restrict,
  monto_aplicado numeric(14,2) not null check (monto_aplicado > 0),
  created_at timestamptz not null default now(),
  constraint uq_pago_factura unique (pago_id, factura_id)
);
drop trigger if exists tg_pago_aplicaciones_validate on ar.pago_aplicaciones;
create trigger tg_pago_aplicaciones_validate
before insert or update on ar.pago_aplicaciones
for each row execute function ar.trg_validate_allocation();
create index if not exists ix_pago_aplicaciones_factura on ar.pago_aplicaciones(factura_id);
create index if not exists ix_pago_aplicaciones_pago on ar.pago_aplicaciones(pago_id);

-- 3.5) Historial de cobranzas (dunning)
create table if not exists ar.historial_cobranzas (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null default ar.current_tenant(),
  cliente_id uuid not null references ar.clientes(id) on delete cascade,
  factura_id uuid references ar.facturas(id) on delete set null,
  fecha timestamptz not null default now(),
  tipo_gestion ar.tipo_gestion not null,
  resultado ar.resultado_gestion,
  observaciones text,
  next_action_at timestamptz,
  created_by uuid references auth.users(id),
  created_at timestamptz not null default now()
);
create index if not exists ix_cobranzas_cliente on ar.historial_cobranzas(cliente_id, fecha desc);
create index if not exists ix_cobranzas_factura on ar.historial_cobranzas(factura_id);

-- 4) Vistas analíticas --------------------------------------
-- 4.1) Saldo por factura
create or replace view ar.vw_saldo_por_factura as
select f.tenant_id,
       f.id as factura_id,
       f.cliente_id,
       f.serie,
       f.numero,
       f.fecha_emision,
       f.fecha_vencimiento,
       f.moneda,
       f.total,
       coalesce(a.aplicado,0)::numeric(14,2) as aplicado,
       (f.total - coalesce(a.aplicado,0))::numeric(14,2) as saldo,
       case when (f.total - coalesce(a.aplicado,0)) <= 0 then true else false end as esta_pagada
from ar.facturas f
left join (
  select factura_id, sum(monto_aplicado) as aplicado
  from ar.pago_aplicaciones
  group by factura_id
) a on a.factura_id = f.id;

-- 4.2) Antigüedad de saldos (aging)
create or replace view ar.vw_antiguedad_saldos as
select c.tenant_id,
       c.id as cliente_id,
       c.razon_social,
       f.moneda,
       sum(case when dias <= 0 then saldo else 0 end) as bucket_no_vencido,
       sum(case when dias between 1 and 30 then saldo else 0 end) as bucket_1_30,
       sum(case when dias between 31 and 60 then saldo else 0 end) as bucket_31_60,
       sum(case when dias between 61 and 90 then saldo else 0 end) as bucket_61_90,
       sum(case when dias > 90 then saldo else 0 end) as bucket_mas_90,
       sum(saldo) as saldo_total
from (
  select v.cliente_id, v.moneda, v.saldo,
         (current_date - v.fecha_vencimiento) as dias
  from ar.vw_saldo_por_factura v
  where v.saldo > 0
) f
join ar.clientes c on c.id = f.cliente_id
group by c.tenant_id, c.id, c.razon_social, f.moneda;

-- 4.3) Estado de cuenta con saldo corrido (por cliente)
create or replace view ar.vw_estado_cuenta as
with movimientos as (
  -- Facturas (cargo)
  select f.tenant_id,
         f.cliente_id,
         f.fecha_emision as fecha,
         ('FAC '||f.serie||'-'||f.numero) as referencia,
         f.total::numeric(14,2) as debe,
         0::numeric(14,2) as haber
  from ar.facturas f
  union all
  -- Aplicaciones de pago (abono)
  select p.tenant_id,
         p.cliente_id,
         p.fecha::timestamp as fecha,
         ('PAGO '||coalesce(p.referencia, p.id::text)) as referencia,
         0::numeric(14,2) as debe,
         pa.monto_aplicado::numeric(14,2) as haber
  from ar.pagos p
  join ar.pago_aplicaciones pa on pa.pago_id = p.id
)
select m.tenant_id,
       m.cliente_id,
       m.fecha,
       m.referencia,
       m.debe,
       m.haber,
       sum(m.debe - m.haber) over (partition by m.tenant_id, m.cliente_id order by m.fecha, m.referencia rows unbounded preceding) as saldo_corrido
from movimientos m
order by m.fecha, m.referencia;

-- 5) RLS (Row Level Security) por tenant ---------------------
alter table ar.clientes enable row level security;
alter table ar.facturas enable row level security;
alter table ar.pagos enable row level security;
alter table ar.pago_aplicaciones enable row level security;
alter table ar.historial_cobranzas enable row level security;

drop policy if exists sel_clientes on ar.clientes;
create policy sel_clientes on ar.clientes
  for select using (tenant_id = ar.current_tenant());
drop policy if exists ins_clientes on ar.clientes;
create policy ins_clientes on ar.clientes
  for insert with check (tenant_id = ar.current_tenant());
drop policy if exists upd_clientes on ar.clientes;
create policy upd_clientes on ar.clientes
  for update using (tenant_id = ar.current_tenant()) with check (tenant_id = ar.current_tenant());
drop policy if exists del_clientes on ar.clientes;
create policy del_clientes on ar.clientes
  for delete using (tenant_id = ar.current_tenant());

drop policy if exists sel_facturas on ar.facturas;
create policy sel_facturas on ar.facturas for select using (tenant_id = ar.current_tenant());
drop policy if exists ins_facturas on ar.facturas;
create policy ins_facturas on ar.facturas for insert with check (tenant_id = ar.current_tenant());
drop policy if exists upd_facturas on ar.facturas;
create policy upd_facturas on ar.facturas for update using (tenant_id = ar.current_tenant()) with check (tenant_id = ar.current_tenant());
drop policy if exists del_facturas on ar.facturas;
create policy del_facturas on ar.facturas for delete using (tenant_id = ar.current_tenant());

drop policy if exists sel_pagos on ar.pagos;
create policy sel_pagos on ar.pagos for select using (tenant_id = ar.current_tenant());
drop policy if exists ins_pagos on ar.pagos;
create policy ins_pagos on ar.pagos for insert with check (tenant_id = ar.current_tenant());
drop policy if exists upd_pagos on ar.pagos;
create policy upd_pagos on ar.pagos for update using (tenant_id = ar.current_tenant()) with check (tenant_id = ar.current_tenant());
drop policy if exists del_pagos on ar.pagos;
create policy del_pagos on ar.pagos for delete using (tenant_id = ar.current_tenant());

drop policy if exists sel_apl on ar.pago_aplicaciones;
create policy sel_apl on ar.pago_aplicaciones for select using (tenant_id = ar.current_tenant());
drop policy if exists ins_apl on ar.pago_aplicaciones;
create policy ins_apl on ar.pago_aplicaciones for insert with check (tenant_id = ar.current_tenant());
drop policy if exists upd_apl on ar.pago_aplicaciones;
create policy upd_apl on ar.pago_aplicaciones for update using (tenant_id = ar.current_tenant()) with check (tenant_id = ar.current_tenant());
drop policy if exists del_apl on ar.pago_aplicaciones;
create policy del_apl on ar.pago_aplicaciones for delete using (tenant_id = ar.current_tenant());

drop policy if exists sel_cob on ar.historial_cobranzas;
create policy sel_cob on ar.historial_cobranzas for select using (tenant_id = ar.current_tenant());
drop policy if exists ins_cob on ar.historial_cobranzas;
create policy ins_cob on ar.historial_cobranzas for insert with check (tenant_id = ar.current_tenant());
drop policy if exists upd_cob on ar.historial_cobranzas;
create policy upd_cob on ar.historial_cobranzas for update using (tenant_id = ar.current_tenant()) with check (tenant_id = ar.current_tenant());
drop policy if exists del_cob on ar.historial_cobranzas;
create policy del_cob on ar.historial_cobranzas for delete using (tenant_id = ar.current_tenant());

-- 6) Datos de ejemplo (prueba rápida) ------------------------
-- Bootstrap de tenant para el SQL Editor (sin JWT)
do $$
begin
  if ar.current_tenant() is null then
    perform set_config('request.jwt.claims', json_build_object('tenant_id', gen_random_uuid())::text, true);
  end if;
end $$;

-- Cliente demo
insert into ar.clientes (tipo_persona, nit, dv, razon_social, email, telefono, ciudad, pais, plazo_dias, cupo_credito)
values ('JURIDICA','900123456',5,'ACME S.A.S.','finanzas@acme.co','3001234567','Bogotá','Colombia',30,50000000)
on conflict (tenant_id, nit) do nothing;

-- Dos facturas para ese cliente
insert into ar.facturas (cliente_id, serie, numero, secuencia, fecha_emision, fecha_vencimiento, moneda, subtotal, impuestos)
select c.id, 'A','0001',1, current_date - 10, current_date - 2, 'COP', 1000000, 190000
from ar.clientes c
where c.nit = '900123456'
on conflict do nothing;

insert into ar.facturas (cliente_id, serie, numero, secuencia, fecha_emision, fecha_vencimiento, moneda, subtotal, impuestos)
select c.id, 'A','0002',2, current_date - 5, current_date + 10, 'COP', 500000, 95000
from ar.clientes c
where c.nit = '900123456'
on conflict do nothing;

-- Un pago demo
insert into ar.pagos (cliente_id, fecha, metodo, referencia, moneda, monto_total, notas)
select c.id, current_date - 1, 'TRANSFERENCIA', 'TRX-001', 'COP', 700000, 'Abono parcial a FAC A-0001'
from ar.clientes c
where c.nit = '900123456'
on conflict do nothing;

-- Aplicación manual (si no ha corrido trigger FIFO aún)
insert into ar.pago_aplicaciones (pago_id, factura_id, monto_aplicado)
select p.id, f.id, 700000
from ar.pagos p
join ar.clientes c on c.id = p.cliente_id and c.nit = '900123456'
join ar.facturas f on f.cliente_id = c.id and f.serie='A' and f.numero='0001'
where p.referencia = 'TRX-001'
  and not exists (
    select 1 from ar.pago_aplicaciones pa where pa.pago_id = p.id and pa.factura_id = f.id
  )
limit 1;

-- 7) Automatización FIFO (bonus) -----------------------------
-- 7.1) Recalcula estado de una factura según saldo
create or replace function ar.recalculate_invoice_status(p_factura_id uuid)
returns void
language plpgsql as $$
declare
  v_estado ar.estado_factura;
  v_total numeric(14,2);
  v_aplicado numeric(14,2);
  v_fecha_venc date;
  v_estado_actual ar.estado_factura;
begin
  select f.total,
         coalesce(a.aplicado,0),
         f.fecha_vencimiento,
         f.estado
  into v_total, v_aplicado, v_fecha_venc, v_estado_actual
  from ar.facturas f
  left join (
    select factura_id, sum(monto_aplicado) as aplicado
    from ar.pago_aplicaciones
    where factura_id = p_factura_id
    group by factura_id
  ) a on a.factura_id = f.id
  where f.id = p_factura_id
  for update;

  if v_estado_actual in ('ANULADA','BORRADOR') then
    return; -- no cambiar estados protegidos
  end if;

  if (v_total - v_aplicado) <= 0 then
    v_estado := 'PAGADA';
  elsif v_aplicado > 0 then
    v_estado := 'PARCIAL';
  else
    if current_date > v_fecha_venc then
      v_estado := 'VENCIDA';
    else
      v_estado := 'EMITIDA';
    end if;
  end if;

  update ar.facturas set estado = v_estado where id = p_factura_id;
end; $$;

-- 7.2) Trigger AFTER INSERT en pagos: valida y aplica FIFO (+ log)
create or replace function ar.trg_auto_apply_payment_fifo()
returns trigger
language plpgsql as $$
declare
  v_outstanding numeric(14,2);
  v_available numeric(14,2);
  v_saldo numeric(14,2);
  v_to_apply numeric(14,2);
  r record;
begin
  -- saldo pendiente del cliente en la misma moneda (facturas abiertas)
  select coalesce(sum(v.saldo),0) into v_outstanding
  from ar.vw_saldo_por_factura v
  join ar.facturas f on f.id = v.factura_id
  where f.cliente_id = new.cliente_id
    and f.moneda = new.moneda
    and f.estado in ('EMITIDA','VENCIDA','PARCIAL')
    and v.saldo > 0;

  if new.monto_total > v_outstanding then
    raise exception 'El monto del pago (%) excede el saldo pendiente del cliente (%).', new.monto_total, v_outstanding;
  end if;

  v_available := new.monto_total;

  for r in
    select f.id as factura_id
    from ar.facturas f
    where f.cliente_id = new.cliente_id
      and f.moneda = new.moneda
      and f.estado in ('EMITIDA','VENCIDA','PARCIAL')
    order by f.fecha_emision asc, f.id asc
  loop
    -- saldo actual de la factura r.factura_id
    select (ff.total - coalesce(sum(pa.monto_aplicado),0))
    into v_saldo
    from ar.facturas ff
    left join ar.pago_aplicaciones pa on pa.factura_id = ff.id
    where ff.id = r.factura_id
    group by ff.total;

    if v_saldo is null or v_saldo <= 0 then
      continue;
    end if;

    v_to_apply := least(v_saldo, v_available);

    insert into ar.pago_aplicaciones (pago_id, factura_id, monto_aplicado)
    values (new.id, r.factura_id, v_to_apply);

    perform ar.recalculate_invoice_status(r.factura_id);

    v_available := v_available - v_to_apply;
    exit when v_available <= 0;
  end loop;

  -- registrar en historial de cobranzas
  insert into ar.historial_cobranzas (tenant_id, cliente_id, factura_id, fecha, tipo_gestion, resultado, observaciones, created_by)
  values (new.tenant_id, new.cliente_id, null, now(), 'ACUERDO_PAGO', 'PAGADO',
          format('Aplicación automática del pago %s por %s %s (FIFO).', new.id, new.monto_total, new.moneda), null);

  return new;
end; $$;

drop trigger if exists tg_pagos_auto_apply on ar.pagos;
create trigger tg_pagos_auto_apply
after insert on ar.pagos
for each row execute function ar.trg_auto_apply_payment_fifo();

-- 8) Consultas útiles ---------------------------------------
-- select * from ar.vw_saldo_por_factura order by fecha_emision;
-- select * from ar.vw_antiguedad_saldos order by razon_social;
-- select * from ar.vw_estado_cuenta order by fecha, referencia;

-- FIN DEL ESQUEMA
